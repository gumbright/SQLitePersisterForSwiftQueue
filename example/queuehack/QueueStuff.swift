//
//  QueueStuff.swift
//  queuehack
//
//  Created by Guy Umbright on 5/9/19.
//  Copyright © 2019 Guy Umbright. All rights reserved.
//

import Foundation
import SwiftQueue

struct MyJobError: Error {
    var storedMessage : String
    init(message:String)
    {
        storedMessage = message
    }
    
    var localizedDescription: String
    {
        return storedMessage
    }
}

let jobURL = "http://127.0.0.1:8000"

class MyJob : Job
{
    static let type = "MyJob"

    let defaultSession = URLSession(configuration: .default)
    var dataTask: URLSessionDataTask?
    let jobNumber : String
    
    required init(params: [String: Any]) {
        jobNumber = params["jobNumber"] as! String
        // Receive params from JobBuilder.with()
        //self.tweet = params
    }

    func onRun(callback: JobResult) {

        dataTask?.cancel()
        
        if Int(arc4random()) % 3 == 1
        {
            print("forced fail")
            callback.done(.fail(MyJobError(message: "forced failure")))
        }
        else
        {
            if var urlComponents = URLComponents(string: jobURL) {
                guard let url = urlComponents.url else { return }
                dataTask = defaultSession.dataTask(with: url) { data, response, error in
                    defer { self.dataTask = nil }
                    if let error = error {
                        print("got error: \(error)")
                        callback.done(.fail(error))
                    } else if let data = data,
                        let response = response as? HTTPURLResponse,
                        response.statusCode == 200 {
                        let resp = String(data: data,encoding: .utf8)
                        //print("got data:\(resp)")
                        DispatchQueue.main.async {
                            NotificationCenter.default.post(name: NSNotification.Name(MyJobNotification), object: self.jobNumber)
                        }
                        callback.done(.success)
                    }
                }
                dataTask?.resume()
            }
        }
    }

    func onRetry(error: Error) -> RetryConstraint {
        // Check if error is non fatal
        //return error is ApiError ? RetryConstraint.cancel : RetryConstraint.retry(delay: 0) // immediate retry
        return RetryConstraint.retry(delay: 0)
    }

    func onRemove(result: JobCompletion) {
        // This job will never run anymore
        //print("job removed")
        switch result {
        case .success:
            // Job success
            break
            
        case .fail(let error):
            // Job fail
            break
            
        }
    }
}

class MyJobCreator : JobCreator
{
    func create(type: String, params: [String: Any]?) -> Job {
        // check for job and params type
        if type == MyJob.type  {
            return MyJob(params: params!)
        } else {
            // Nothing match
            // You can use `fatalError` or create an empty job to report this issue.
            fatalError("No Job !")
        }
    }

}

//let manager = SwiftQueueManagerBuilder(creator: TweetJobCreator()).build()

/*
 JobBuilder(type: SendTweetJob.type)
 // Requires internet to run
 .internet(atLeast: .cellular)
 // params of my job
 .with(params: ["content": "Hello world"])
 // Add to queue manager
 .schedule(manager: manager)
 */
