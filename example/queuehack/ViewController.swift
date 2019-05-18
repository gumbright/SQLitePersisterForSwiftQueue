//
//  ViewController.swift
//  queuehack
//
//  Created by Guy Umbright on 5/9/19.
//  Copyright © 2019 Guy Umbright. All rights reserved.
//

import UIKit
import SwiftQueue

let MyJobNotification = "MyJobNotification"
let delay = 15.0

class ViewController: UIViewController {
    
    var jobNumber = 0
    @IBOutlet weak var tableView: UITableView!
    var observer:NSObjectProtocol?
    var completedJobs : [String] = []
    
    public let manager = SwiftQueueManagerBuilder(creator: MyJobCreator()).set(persister:SQLitePersister(key: "test")).build()

    override func viewDidLoad() {
        super.viewDidLoad()
        
        let observer = NotificationCenter.default.addObserver(forName: NSNotification.Name(MyJobNotification), object: nil, queue: nil) { (notification) in
            let s = notification.object as! String
            if self.completedJobs.count > 10
            {
                self.completedJobs.removeFirst()
            }
            self.completedJobs.append(s)
            DispatchQueue.main.async {
                self.tableView.reloadData()
            }
        }
    }

    @IBAction func createJobPressed(_ sender: Any) {
        JobBuilder(type: MyJob.type)
            // Requires internet to run
            .internet(atLeast: .cellular)
            .with(params: ["jobNumber": "\(jobNumber)"])
            .delay(time:delay)
            .persist(required: true)
            .retry(limit: .unlimited)
            .schedule(manager: manager)
            jobNumber += 1
    }
}

extension ViewController : UITableViewDelegate
{
}

extension ViewController : UITableViewDataSource
{
    func numberOfSections(in tableView: UITableView) -> Int {
        return 1
    }
    
    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        return completedJobs.count
    }
    
    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        let cell = tableView.dequeueReusableCell(withIdentifier: "JobCell", for: indexPath)
        cell.textLabel?.text = completedJobs[indexPath.row]
        return cell
    }
}
