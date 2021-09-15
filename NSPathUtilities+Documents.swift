//
//  NSPathUtilities+Documents.swift
//  queuehack
//
//  Created by Pierre Chardonneau on 31/08/2021.
//  Copyright © 2021 pch. All rights reserved.
//

import Foundation

public func documentsPath() -> String {
    NSSearchPathForDirectoriesInDomains(
        .documentDirectory,
        .userDomainMask,
        true).first!
}
