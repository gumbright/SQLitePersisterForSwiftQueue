//
//  SQLitePersister.swift
//  queuehack
//
//  Created by Guy Umbright on 5/10/19.
//  Copyright © 2019 Guy Umbright. All rights reserved.
//

import SQLite3
import SwiftQueue

class SQLitePersister: JobPersister {
    private static let tableName = "QueuedJobs"
    
    private var createTableQuery: String { """
        CREATE TABLE IF NOT EXISTS \(SQLitePersister.tableName) (
            queueName TEXT NOT NULL,
            taskId TEXT NOT NULL,
            jobInfo TEXT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique ON \(SQLitePersister.tableName) (queueName, taskId)
        """}
    
    private var database: OpaquePointer?
    private var dbFileURL: URL?
    private var tableCreated = false
    
    private var getQueuesNameStmt: OpaquePointer?
    private var getJobForQueueStmt: OpaquePointer?
    private var insertStmt: OpaquePointer?
    private var deleteStmt: OpaquePointer?
    private var deleteAllStmt: OpaquePointer?
    
    private let persisterQueue = DispatchQueue(label: "persisterQueue", qos: .userInitiated)
    
    private let dbName: String
    
    // MARK: - Loggings
    private func logTrace(_ str: String) {
        print("SQLitePersister - TRACE: \(str)")
    }
    
    private func logError(_ str: String) {
        print("SQLitePersister - ERROR: \(str)")
    }
    
    // MARK: - Life cycle
    /// Create a Job persister with a custom dbName
    init(dbName: String = "default") {
        self.dbName = dbName
        
        logTrace("persister init \"\(dbName)\"")
        setupDatabase()
    }
    
    deinit {
        logTrace("persister deinit")
        
        closeDatabase()
        clearPreparedStmt()
    }
    
    // MARK: - JobPersister implementations
    // Structure as follow: [group:[id:data]]
    func restore() -> [String] {
        let queueNames = getQueueNames()
        logTrace("restore \(queueNames.count) queues found")
        return queueNames
    }
    
    /// Restore jobs for a single queue
    /// Returns an array of String. serialized job
    func restore(queueName: String) -> [String] {
        let jobs = restoreJobsForQueue(queueName: queueName)
        logTrace("restored for queue \"\(queueName)\": \(jobs.count) job(s)")
        return jobs
    }
    
    /// Insert a job to a specific queue
    func put(queueName: String, taskId: String, data: String) {
        logTrace("put queueName: \(queueName) taskId: \(taskId) data: \(data)")
        persisterQueue.async {
            let result = self.insertJob(queueName: queueName, taskId: taskId, jobInfo: data)
            self.logTrace("put result: \(result)")
        }
    }
    
    /// Remove a specific task from a queue
    func remove(queueName: String, taskId: String) {
        logTrace("remove queueName: \(queueName) taskId: \(taskId)")
        persisterQueue.async {
            self.removeJob(queueName: queueName, taskId: taskId)
        }
    }
    
    func clearAll() {
        logTrace("clearAll")
        persisterQueue.async {
            self.removeAllJobs()
        }
    }
    
    // MARK: - Prepare Statments
    private func prepareGetQueuesNameStmt() -> Bool {
        guard getQueuesNameStmt == nil else { return true }
        
        var result = true
        let sql = "SELECT DISTINCT queueName FROM \(SQLitePersister.tableName)"
        if sqlite3_prepare_v2(database, sql, -1, &getQueuesNameStmt, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("error preparing getQueuesStmt: \(errmsg)")
            result = false
        }
        return result
    }
    
    private func prepareGetJobForQueueStmt() -> Bool {
        guard getJobForQueueStmt == nil else { return true }
        
        var result = true
        let sql = "SELECT queueName,taskId,jobInfo FROM \(SQLitePersister.tableName) WHERE queueName = ?"
        if sqlite3_prepare_v2(database, sql, -1, &getJobForQueueStmt, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("error preparing getJobForQueueStmt: \(errmsg)")
            result = false
        }
        return result
    }
    
    private func prepareInsertStmt() -> Bool {
        guard insertStmt == nil else { return true }
        
        var result = true
        let sql = "INSERT OR REPLACE INTO \(SQLitePersister.tableName) (queueName, taskId, jobInfo) VALUES (?,?,?)"
        if sqlite3_prepare_v2(database, sql, -1, &insertStmt, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("error preparing insertStmt: \(errmsg)")
            result = false
        }
        return result
    }
    
    private func prepareDeleteStmt() -> Bool {
        guard deleteStmt == nil else { return true }
        
        var result = true
        let sql = "DELETE FROM \(SQLitePersister.tableName) WHERE queueName = ? AND taskId = ?"
        if sqlite3_prepare_v2(database, sql, -1, &deleteStmt, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("error preparing deleteStmt: \(errmsg)")
            result = false
        }
        return result
    }
    
    private func prepareDeleteAllStmt() -> Bool {
        guard deleteAllStmt == nil else { return true }
        
        var result = true
        let sql = "DROP TABLE \(SQLitePersister.tableName)"
        if sqlite3_prepare_v2(database, sql, -1, &deleteAllStmt, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("error preparing deleteAllStmt: \(errmsg)")
            result = false
        }
        return result
    }
    
    // MARK: - Private methods
    private func setupDatabase() {
        // create the application support dir if needed
        let documentsURL = URL(fileURLWithPath: documentsPath())
        let sqliteFileURL = documentsURL.appendingPathComponent("SQLitePersister").appendingPathComponent("\(dbName).sqlite")
        let sqliteDirectoryUrl = sqliteFileURL.deletingLastPathComponent()
        
        if !FileManager.default.fileExists(atPath: sqliteDirectoryUrl.absoluteString) {
            do {
                try FileManager.default.createDirectory(at: sqliteDirectoryUrl, withIntermediateDirectories: true, attributes: nil)
            } catch {
                logError("fail to create database directory: \(error)")
            }
        }
        
        dbFileURL = sqliteFileURL
    }
    
    private func openDatabase() -> Bool {
        var result = false
        
        if let url = dbFileURL {
            let openResult = sqlite3_open_v2(url.path, &database, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
            if openResult == SQLITE_OK {
                result = createTable()
            } else {
                let errmsg = String(cString: sqlite3_errmsg(database)!)
                logError("error opening database: \(errmsg)")
            }
        }
        
        return result
    }
    
    @discardableResult
    private func closeDatabase() -> Bool {
        var result = true
        
        if sqlite3_close_v2(database) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("error closing database: \(errmsg)")
            result = false
        }
        
        database = nil
        
        return result
    }
    
    private func createTable() -> Bool {
        if !tableCreated {
            let createResult = sqlite3_exec(database, createTableQuery, nil, nil, nil)
            
            if createResult != SQLITE_OK {
                let errmsg = String(cString: sqlite3_errmsg(database)!)
                logError("error creating table: \(errmsg)")
            } else {
                tableCreated = true
            }
        }
        
        return tableCreated
    }
    
    private func getQueueNames() -> [String] {
        var queueNames: [String] = []
        
        guard openDatabase(), prepareGetQueuesNameStmt() else { return queueNames }
        
        defer {
            sqlite3_reset(getQueuesNameStmt)
            closeDatabase()
        }
        
        while sqlite3_step(getQueuesNameStmt) == SQLITE_ROW {
            let queueName = String(cString: sqlite3_column_text(getQueuesNameStmt, 0))
            queueNames.append(queueName)
        }
        
        return queueNames
    }
    
    private func restoreJobsForQueue(queueName: String) -> [String] {
        var jobData: [String] = []
        
        guard openDatabase(), prepareGetJobForQueueStmt() else { return jobData }
        
        defer {
            sqlite3_reset(getJobForQueueStmt)
            closeDatabase()
        }
        
        if sqlite3_bind_text(getJobForQueueStmt, 1, (queueName as NSString).utf8String, -1, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("error binding `queueName` for getJobForQueueStmt: \(errmsg)")
        } else {
            // traversing through all the records
            var stepResult = sqlite3_step(getJobForQueueStmt)
            while stepResult == SQLITE_ROW {
                let data = String(cString: sqlite3_column_text(getJobForQueueStmt, 2))
                jobData.append(data)
                stepResult = sqlite3_step(getJobForQueueStmt)
            }
        }
        
        return jobData
    }
    
    private func insertJob(queueName: String, taskId: String, jobInfo: String) -> Bool {
        guard openDatabase(), prepareInsertStmt() else { return false }
        
        defer {
            sqlite3_reset(insertStmt)
            closeDatabase()
        }
        
        var result = true
        
        // binding the parameters
        if result && sqlite3_bind_text(insertStmt, 1, (queueName as NSString).utf8String, -1, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("failure binding queueName: \(errmsg)")
            result = false
        }
        
        if result && sqlite3_bind_text(insertStmt, 2, (taskId as NSString).utf8String, -1, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("failure binding taskId: \(errmsg)")
            result = false
        }
        
        if result && sqlite3_bind_text(insertStmt, 3, (jobInfo as NSString).utf8String, -1, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("failure binding jobInfo: \(errmsg)")
            result = false
        }
        
        // executing the query to insert values
        if result && sqlite3_step(insertStmt) != SQLITE_DONE {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("failure inserting job: \(errmsg)")
            result = false
        }
        
        return result
    }
    
    private func removeJob(queueName: String, taskId: String) {
        guard openDatabase(), prepareDeleteStmt() else { return }
        
        defer {
            sqlite3_reset(deleteStmt)
            closeDatabase()
        }
        
        if sqlite3_bind_text(deleteStmt, 1, queueName, -1, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("failure binding name: \(errmsg)")
        }
        
        if sqlite3_bind_text(deleteStmt, 2, taskId, -1, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("failure binding name: \(errmsg)")
        }
        
        if sqlite3_step(deleteStmt) == SQLITE_DONE {
            logTrace("Successfully deleted row.")
        } else {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("Could not delete row: \(errmsg)")
        }
    }
    
    private func removeAllJobs() {
        guard openDatabase(), prepareDeleteAllStmt() else { return }
        
        defer {
            sqlite3_reset(deleteAllStmt)
            closeDatabase()
        }
        
        if sqlite3_step(deleteAllStmt) == SQLITE_DONE {
            logTrace("Successfully deleted all rows.")
            tableCreated = false
            _ = createTable()
        } else {
            let errmsg = String(cString: sqlite3_errmsg(database)!)
            logError("Could not delete all rows: \(errmsg)")
        }
    }
    
    private func clearPreparedStmt() {
        sqlite3_finalize(getQueuesNameStmt)
        sqlite3_finalize(getJobForQueueStmt)
        sqlite3_finalize(insertStmt)
        sqlite3_finalize(deleteStmt)
        sqlite3_finalize(deleteAllStmt)
        
        getQueuesNameStmt = nil
        getJobForQueueStmt = nil
        insertStmt = nil
        deleteStmt = nil
        deleteAllStmt = nil
    }
}
