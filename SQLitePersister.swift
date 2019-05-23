//
//  SQLitePersister.swift
//  queuehack
//
//  Created by Guy Umbright on 5/10/19.
//  Copyright © 2019 Guy Umbright. All rights reserved.
//

import Foundation
import SwiftQueue
import SQLite3

class SQLitePersister : JobPersister {
    var db: OpaquePointer?
    var dbFileURL:URL?
    var tableCreated = false
    
    let persisterQueue = DispatchQueue(label: "persisterQueue", qos: .userInitiated)
    
    private let key: String
    let insertQuery = "INSERT INTO QueuedJobs (queueName, taskId, jobInfo) VALUES (?,?,?)"
    let deleteQuery = "DELETE FROM QueuedJobs WHERE queueName = ? AND taskId = ?"
    let jobsForQueueQuery = "SELECT id,queueName,taskId,jobInfo FROM QueuedJobs WHERE queueName = '<queuename>'"
    let restoreQuery = "SELECT DISTINCT queueName FROM QueuedJobs"
    let createTableQuery = "CREATE TABLE IF NOT EXISTS QueuedJobs (id INTEGER PRIMARY KEY AUTOINCREMENT, queueName TEXT NOT NULL, taskId TEXT NOT NULL, jobInfo TEXT NOT NULL)"

    /// Create a Job persister with a custom key
    public init(key: String = "SQLitePersister") {
        self.key = key
        setupDatabase()
    }

    deinit {
        print("persister deinit")
    }
    
    func setupDatabase()
    {
        //create the application support dir if needed
        let fileManager = FileManager.default
        let urls = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask)
        if let applicationSupportURL = urls.last {
            do {
                try fileManager.createDirectory(at: applicationSupportURL as URL, withIntermediateDirectories: true, attributes: nil)
            }
            catch{
                print("createDirectory failed")
            }
        }
        
        dbFileURL = try! FileManager.default.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
            .appendingPathComponent("SQLitePersister.sqlite")  //!!!probs want to be able to pass this in
    }

    func openDatabase()
    {
        if let url = dbFileURL
        {
            let openResult = sqlite3_open_v2(url.path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
            if openResult == SQLITE_OK {
                createTable()
            }
            else
            {
                let errmsg = String(cString: sqlite3_errmsg(db)!)
                print("error opening database: \(errmsg)")

            }
        }
    }

    func closeDatabase()
    {
        if sqlite3_close_v2(db) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("error closing database: \(errmsg)")
        }
        
        db = nil
    }
    
    func createTable()
    {
        if tableCreated == false
        {
            let createResult = sqlite3_exec(db, createTableQuery, nil, nil, nil)
        
            if createResult != SQLITE_OK {
                let errmsg = String(cString: sqlite3_errmsg(db)!)
                print("error creating table: \(errmsg)")
            }
            else
            {
                tableCreated = true
            }
        }
    }
    
    // Structure as follow
    // [group:[id:data]]
    public func restore() -> [String] {
        //print("SQLitePersister-restore")
        let queueNames = getQueueNames()
        //print("SQLitePersister-restore \(queueNames.count) queues found")
        return queueNames
    }
    
    func getQueueNames() -> [String]
    {
        var queueNames : [String] = []
        
        var stmt:OpaquePointer?
        
        openDatabase()
        //preparing the query
        if sqlite3_prepare_v2(db, restoreQuery, -1, &stmt, nil) != SQLITE_OK{
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("error preparing insert: \(errmsg)")
        }
        else
        {
            while(sqlite3_step(stmt) == SQLITE_ROW)
            {
                let queueName = String(cString: sqlite3_column_text(stmt, 0))
                queueNames.append(queueName)
            }
        }
        
        sqlite3_finalize(stmt)
        closeDatabase()
        return queueNames
        
    }

    /// Restore jobs for a single queue
    /// Returns an array of String. serialized job
    public func restore(queueName: String) -> [String] {
        //print("SQLitePersister-restore queueName: \(queueName)")
        let jobs = restoreJobsForQueue(queueName: queueName)
        //print("SQLitePersister-restored \(jobs.count) jobs")
        return jobs
    }
    
    func restoreJobsForQueue(queueName:String) -> [String]
    {
        var stmt:OpaquePointer?
        var jobData : [String] = []

        openDatabase()
        
        //preparing the query
        var query = jobsForQueueQuery
        query = query.replacingOccurrences(of: "<queuename>", with: queueName)
        
        if sqlite3_prepare_v2(db, query, -1, &stmt, nil) != SQLITE_OK{
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("error preparing insert: \(errmsg)")
        }
        else
        {
            //traversing through all the records
            var stepResult = sqlite3_step(stmt)
            while(stepResult == SQLITE_ROW){
                let data = String(cString: sqlite3_column_text(stmt, 3))
                jobData.append(data)
                stepResult = sqlite3_step(stmt)
                
            }
        }

        sqlite3_finalize(stmt)
        closeDatabase()
        return jobData
    }
    
    /// Insert a job to a specific queue
    public func put(queueName: String, taskId: String, data: String) {
        //print("SQLitePersister-put queueName: \(queueName) taskId:\(taskId) data:\(data)")
        persisterQueue.async {
            let result = self.insertJob(queueName: queueName, taskId: taskId, jobInfo: data)
            //print("SQLitePersister-put result: \(result)")
        }
   }

    
    func insertJob(queueName:String, taskId:String, jobInfo:String) -> Bool
    {
        let queryString = insertQuery
        var stmt: OpaquePointer?
        var result = true
        
        openDatabase()
        if sqlite3_prepare_v2(db, queryString, -1, &stmt, nil) != SQLITE_OK{
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("error preparing insert: \(errmsg)")
            result =  false
        }
        
        //binding the parameters
        if result == true && sqlite3_bind_text(stmt, 1, queueName, -1, nil) != SQLITE_OK{
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("failure binding name: \(errmsg)")
            result = false
        }
        
        if result == true && sqlite3_bind_text(stmt, 2, taskId, -1, nil) != SQLITE_OK{
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("failure binding name: \(errmsg)")
            result = false
        }
        
        if result == true && sqlite3_bind_text(stmt, 3, jobInfo, -1, nil) != SQLITE_OK{
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("failure binding name: \(errmsg)")
            result = false
        }
        
        //executing the query to insert values
        if result == true && sqlite3_step(stmt) != SQLITE_DONE {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("failure inserting job: \(errmsg)")
            result = false
        }
        
        sqlite3_finalize(stmt)

        closeDatabase()
        return result
    }
    
    /// Remove a specific task from a queue
    public func remove(queueName: String, taskId: String) {
        print("SQLitePersister-remove queueName: \(queueName) taskId:\(taskId)")
        persisterQueue.async {
            self.removeJob(queueName: queueName, taskId: taskId)
        }
    }

    func removeJob(queueName:String, taskId:String)
    {
        var deleteStatement: OpaquePointer? = nil
        
        openDatabase()
        if sqlite3_prepare_v2(db, deleteQuery, -1, &deleteStatement, nil) == SQLITE_OK {
            
            if sqlite3_bind_text(deleteStatement, 1, queueName, -1, nil) != SQLITE_OK{
                let errmsg = String(cString: sqlite3_errmsg(db)!)
                print("failure binding name: \(errmsg)")
            }
            
            if sqlite3_bind_text(deleteStatement, 2, taskId, -1, nil) != SQLITE_OK{
                let errmsg = String(cString: sqlite3_errmsg(db)!)
                print("failure binding name: \(errmsg)")
            }

            if sqlite3_step(deleteStatement) == SQLITE_DONE {
                print("Successfully deleted row.")
            } else {
                let errmsg = String(cString: sqlite3_errmsg(db)!)
                print("Could not delete row: \(errmsg)")
            }
        } else {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("DELETE statement could not be prepared:\(errmsg)")
        }
        
        sqlite3_finalize(deleteStatement)
        closeDatabase()
    }
}

