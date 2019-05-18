//
//  queuehackTests.swift
//  queuehackTests
//
//  Created by Guy Umbright on 5/10/19.
//  Copyright © 2019 Guy Umbright. All rights reserved.
//

import XCTest
import SwiftQueue

class sqlitePersisterTests: XCTestCase {

    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testAdd()
    {
        print("create persister")
        let persister = SQLitePersister(key: "unittest")
        print("pre put")
        persister.put(queueName: "Apple", taskId: "1", data: "This is some data")
        print("post put")

    }
/*
    func testPerformanceExample() {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
        }
    }
*/
}
