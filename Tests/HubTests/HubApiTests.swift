//
//  HubApiTests.swift
//
//  Created by Pedro Cuenca on 20231230.
//

@testable import Hub
import XCTest

class HubApiTests: XCTestCase {
    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    // MARK: use a specific revision for these tests

    func testFilenameRetrieval() async {
        do {
            let filenames = try await Hub.getFilenames(from: "coreml-projects/Llama-2-7b-chat-coreml")
            XCTAssertEqual(filenames.count, 13)
        } catch {
            XCTFail("\(error)")
        }
    }

    func testFilenameRetrievalWithGlob() async {
        do {
            try await {
                let filenames = try await Hub.getFilenames(from: "coreml-projects/Llama-2-7b-chat-coreml", matching: "*.json")
                XCTAssertEqual(
                    Set(filenames),
                    Set([
                        "config.json", "tokenizer.json", "tokenizer_config.json",
                        "llama-2-7b-chat.mlpackage/Manifest.json",
                        "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json",
                        "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json",
                    ])
                )
            }()

            // Glob patterns are case sensitive
            try await {
                let filenames = try await Hub.getFilenames(from: "coreml-projects/Llama-2-7b-chat-coreml", matching: "*.JSON")
                XCTAssertEqual(
                    filenames,
                    []
                )
            }()
        } catch {
            XCTFail("\(error)")
        }
    }

    func testFilenameRetrievalFromDirectories() async {
        do {
            // Contents of all directories matching a pattern
            let filenames = try await Hub.getFilenames(from: "coreml-projects/Llama-2-7b-chat-coreml", matching: "*.mlpackage/*")
            XCTAssertEqual(
                Set(filenames),
                Set([
                    "llama-2-7b-chat.mlpackage/Manifest.json",
                    "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json",
                    "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json",
                    "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel",
                    "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/weights/weight.bin",

                ])
            )
        } catch {
            XCTFail("\(error)")
        }
    }

    func testFilenameRetrievalWithMultiplePatterns() async {
        do {
            let patterns = ["config.json", "tokenizer.json", "tokenizer_*.json"]
            let filenames = try await Hub.getFilenames(from: "coreml-projects/Llama-2-7b-chat-coreml", matching: patterns)
            XCTAssertEqual(
                Set(filenames),
                Set(["config.json", "tokenizer.json", "tokenizer_config.json"])
            )
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testGetFileMetadata() async throws {
        do {
            let url = URL(string: "https://huggingface.co/coreml-projects/Llama-2-7b-chat-coreml/resolve/main/config.json")
            let metadata = try await Hub.getFileMetadata(fileURL: url!)
            
            XCTAssertNotNil(metadata.commitHash)
            XCTAssertNotNil(metadata.etag)
            XCTAssertEqual(metadata.location, url?.absoluteString)
            XCTAssertEqual(metadata.size, 163)
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testGetFileMetadataBlobPath() async throws {
        do {
            let url = URL(string: "https://huggingface.co/coreml-projects/Llama-2-7b-chat-coreml/resolve/main/config.json")
            let metadata = try await Hub.getFileMetadata(fileURL: url!)
            
            XCTAssertNotNil(metadata.commitHash)
            XCTAssertTrue(metadata.etag != nil && metadata.etag!.hasPrefix("d6ceb9"))
            XCTAssertEqual(metadata.location, url?.absoluteString)
            XCTAssertEqual(metadata.size, 163)
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testGetFileMetadataWithRevision() async throws {
        do {
            let revision = "f2c752cfc5c0ab6f4bdec59acea69eefbee381c2"
            let url = URL(string: "https://huggingface.co/julien-c/dummy-unknown/resolve/\(revision)/config.json")
            let metadata = try await Hub.getFileMetadata(fileURL: url!)
            
            XCTAssertEqual(metadata.commitHash, revision)
            XCTAssertNotNil(metadata.etag)
            XCTAssertGreaterThan(metadata.etag!.count, 0)
            XCTAssertEqual(metadata.location, url?.absoluteString)
            XCTAssertEqual(metadata.size, 851)
        } catch {
            XCTFail("\(error)")
        }
    }

    func testGetFileMetadataWithBlobSearch() async throws {
        let repo = "coreml-projects/Llama-2-7b-chat-coreml"
        let metadataFromBlob = try await Hub.getFileMetadata(from: repo, matching: "*.json").sorted { $0.location < $1.location }
        let files = try await Hub.getFilenames(from: repo, matching: "*.json").sorted()
        for (metadata, file) in zip(metadataFromBlob, files) {
            XCTAssertNotNil(metadata.commitHash)
            XCTAssertNotNil(metadata.etag)
            XCTAssertGreaterThan(metadata.etag!.count, 0)
            XCTAssertTrue(metadata.location.contains(file))
            XCTAssertGreaterThan(metadata.size!, 0)
        }
    }
    
    /// Verify with `curl -I https://huggingface.co/coreml-projects/Llama-2-7b-chat-coreml/resolve/main/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel`
    func testGetLargeFileMetadata() async throws {
        do {
            let revision = "eaf97358a37d03fd48e5a87d15aff2e8423c1afb"
            let etag = "fc329090bfbb2570382c9af997cffd5f4b78b39b8aeca62076db69534e020107"
            let location = "https://cdn-lfs.hf.co/repos/4a/4e/4a4e587f66a2979dcd75e1d7324df8ee9ef74be3582a05bea31c2c26d0d467d0/fc329090bfbb2570382c9af997cffd5f4b78b39b8aeca62076db69534e020107?response-content-disposition=inline%3B+filename*%3DUTF-8%27%27model.mlmodel%3B+filename%3D%22model.mlmodel"
            let size = 504766
            
            let url = URL(string: "https://huggingface.co/coreml-projects/Llama-2-7b-chat-coreml/resolve/main/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel")
            let metadata = try await Hub.getFileMetadata(fileURL: url!)
                        
            XCTAssertEqual(metadata.commitHash, revision)
            XCTAssertNotNil(metadata.etag, etag)
            XCTAssertTrue(metadata.location.contains(location))
            XCTAssertEqual(metadata.size, size)
        } catch {
            XCTFail("\(error)")
        }
    }
}

class SnapshotDownloadTests: XCTestCase {
    var repo = "coreml-projects/Llama-2-7b-chat-coreml"
    let downloadDestination: URL = {
        let base = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first!
        return base.appending(component: "huggingface-tests")
    }()

    override func setUp() {}

    override func tearDown() {
        do {
            try FileManager.default.removeItem(at: downloadDestination)
        } catch {
            print("Can't remove test download destination \(downloadDestination), error: \(error)")
        }
    }

    func getRelativeFiles(url: URL) -> [String] {
        var filenames: [String] = []
        let prefix = downloadDestination.appending(path: "models/\(repo)").path.appending("/")

        if let enumerator = FileManager.default.enumerator(at: url, includingPropertiesForKeys: [.isRegularFileKey], options: [.skipsHiddenFiles], errorHandler: nil) {
            for case let fileURL as URL in enumerator {
                do {
                    let resourceValues = try fileURL.resourceValues(forKeys: [.isRegularFileKey])
                    if resourceValues.isRegularFile == true {
                        filenames.append(String(fileURL.path.suffix(from: prefix.endIndex)))
                    }
                } catch {
                    print("Error reading file resources: \(error)")
                }
            }
        }
        return filenames
    }

    func testDownload() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination)
        var lastProgress: Progress? = nil
        
        // Add debug prints
        print("Download destination before: \(downloadDestination.path)")
        
        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "*.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        // Add more debug prints
        print("Downloaded to: \(downloadedTo.path)")
        
        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        print("Downloaded filenames: \(downloadedFilenames)")
        print("Prefix used in getRelativeFiles: \(downloadDestination.appending(path: "models/\(repo)").path)")
        
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 6)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))

        XCTAssertEqual(
            Set(downloadedFilenames),
            Set([
                "config.json", "tokenizer.json", "tokenizer_config.json",
                "llama-2-7b-chat.mlpackage/Manifest.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json",
            ])
        )
    }

    /// Background sessions get rate limited by the OS, see discussion here: https://github.com/huggingface/swift-transformers/issues/61
    /// Test only one file at a time
    func testDownloadInBackground() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination, useBackgroundSession: true)
        var lastProgress: Progress? = nil
        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 1)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))

        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        XCTAssertEqual(
            Set(downloadedFilenames),
            Set([
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json",
            ])
        )
    }

    func testCustomEndpointDownload() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination, endpoint: "https://hf-mirror.com")
        var lastProgress: Progress? = nil
        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "*.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 6)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))

        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        XCTAssertEqual(
            Set(downloadedFilenames),
            Set([
                "config.json", "tokenizer.json", "tokenizer_config.json",
                "llama-2-7b-chat.mlpackage/Manifest.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json",
            ])
        )
    }
    
    func testDownloadFileMetadata() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination)
        var lastProgress: Progress? = nil
        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "*.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 6)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))
        
        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        XCTAssertEqual(
            Set(downloadedFilenames),
            Set([
                "config.json", "tokenizer.json", "tokenizer_config.json",
                "llama-2-7b-chat.mlpackage/Manifest.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json",
            ])
        )
        
        let metadataDestination = downloadedTo.appending(component: ".cache/huggingface/download")
        let downloadedMetadataFilenames = getRelativeFiles(url: metadataDestination)
        XCTAssertEqual(
            Set(downloadedMetadataFilenames),
            Set([
                ".cache/huggingface/download/config.json.metadata",
                ".cache/huggingface/download/tokenizer.json.metadata",
                ".cache/huggingface/download/tokenizer_config.json.metadata",
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Manifest.json.metadata",
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json.metadata",
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json.metadata",
            ])
        )
    }
    
    func testDownloadFileMetadataExists() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination)
        var lastProgress: Progress? = nil

        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "*.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 6)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))

        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        XCTAssertEqual(
            Set(downloadedFilenames),
            Set([
                "config.json", "tokenizer.json", "tokenizer_config.json",
                "llama-2-7b-chat.mlpackage/Manifest.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json",
            ])
        )
        
        let metadataDestination = downloadedTo.appending(component: ".cache/huggingface/download")
        
        let configPath = downloadedTo.appending(path: "config.json")
        var attributes = try FileManager.default.attributesOfItem(atPath: configPath.path)
        let originalTimestamp = attributes[.modificationDate] as! Date
        
        let downloadedMetadataFilenames = getRelativeFiles(url: metadataDestination)
        XCTAssertEqual(
            Set(downloadedMetadataFilenames),
            Set([
                ".cache/huggingface/download/config.json.metadata",
                ".cache/huggingface/download/tokenizer.json.metadata",
                ".cache/huggingface/download/tokenizer_config.json.metadata",
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Manifest.json.metadata",
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json.metadata",
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json.metadata",
            ])
        )
        
        let _ = try await hubApi.snapshot(from: repo, matching: "*.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        attributes = try FileManager.default.attributesOfItem(atPath: configPath.path)
        let secondDownloadTimestamp = attributes[.modificationDate] as! Date

        // File will not be downloaded again thus last modified date will remain unchanged
        XCTAssertTrue(originalTimestamp == secondDownloadTimestamp)
    }
    
    func testDownloadFileMetadataSame() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination)
        var lastProgress: Progress? = nil

        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "tokenizer.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 1)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))

        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        XCTAssertEqual(Set(downloadedFilenames), Set(["tokenizer.json"]))
        
        let metadataDestination = downloadedTo.appending(component: ".cache/huggingface/download")
        
        let metadataPath = metadataDestination.appending(path: "tokenizer.json.metadata")
        
        let downloadedMetadataFilenames = getRelativeFiles(url: metadataDestination)
        XCTAssertEqual(
            Set(downloadedMetadataFilenames),
            Set([
                ".cache/huggingface/download/tokenizer.json.metadata",
            ])
        )
        
        let originalMetadata = try String(contentsOf: metadataPath, encoding: .utf8)
        
        let _ = try await hubApi.snapshot(from: repo, matching: "tokenizer.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        let secondDownloadMetadata = try String(contentsOf: metadataPath, encoding: .utf8)
        
        // File hasn't changed so commit hash and etag will be identical
        let originalArr = originalMetadata.components(separatedBy: .newlines)
        let secondDownloadArr = secondDownloadMetadata.components(separatedBy: .newlines)
        
        XCTAssertTrue(originalArr[0] == secondDownloadArr[0])
        XCTAssertTrue(originalArr[1] == secondDownloadArr[1])
    }
    
    func testDownloadFileMetadataCorrupted() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination)
        var lastProgress: Progress? = nil

        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "*.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 6)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))

        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        XCTAssertEqual(
            Set(downloadedFilenames),
            Set([
                "config.json", "tokenizer.json", "tokenizer_config.json",
                "llama-2-7b-chat.mlpackage/Manifest.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json",
                "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json",
            ])
        )
        
        let metadataDestination = downloadedTo.appending(component: ".cache/huggingface/download")
        
        let configPath = downloadedTo.appending(path: "config.json")
        var attributes = try FileManager.default.attributesOfItem(atPath: configPath.path)
        let originalTimestamp = attributes[.modificationDate] as! Date
        
        let downloadedMetadataFilenames = getRelativeFiles(url: metadataDestination)
        XCTAssertEqual(
            Set(downloadedMetadataFilenames),
            Set([
                ".cache/huggingface/download/config.json.metadata",
                ".cache/huggingface/download/tokenizer.json.metadata",
                ".cache/huggingface/download/tokenizer_config.json.metadata",
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Manifest.json.metadata",
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/FeatureDescriptions.json.metadata",
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/Metadata.json.metadata",
            ])
        )
        
        // Corrupt config.metadata
        print("Testing corrupted file.")
        try "a".write(to: metadataDestination.appendingPathComponent("config.json.metadata"), atomically: true, encoding: .utf8)
        
        let _ = try await hubApi.snapshot(from: repo, matching: "*.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        attributes = try FileManager.default.attributesOfItem(atPath: configPath.path)
        let secondDownloadTimestamp = attributes[.modificationDate] as! Date

        // File will be downloaded again thus last modified date will change
        XCTAssertTrue(originalTimestamp != secondDownloadTimestamp)
        
        // Corrupt config.metadata again
        print("Testing corrupted timestamp.")
        try "a\nb\nc\n".write(to: metadataDestination.appendingPathComponent("config.json.metadata"), atomically: true, encoding: .utf8)
        
        let _ = try await hubApi.snapshot(from: repo, matching: "*.json") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        attributes = try FileManager.default.attributesOfItem(atPath: configPath.path)
        let thirdDownloadTimestamp = attributes[.modificationDate] as! Date

        // File will be downloaded again thus last modified date will change
        XCTAssertTrue(originalTimestamp != thirdDownloadTimestamp)
    }
    
    func testDownloadLargeFileMetadataCorrupted() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination)
        var lastProgress: Progress? = nil

        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "*.mlmodel") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 1)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))

        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        XCTAssertEqual(
            Set(downloadedFilenames),
            Set(["llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel"])
        )
        
        let metadataDestination = downloadedTo.appending(component: ".cache/huggingface/download")
        
        let modelPath = downloadedTo.appending(path: "llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel")
        var attributes = try FileManager.default.attributesOfItem(atPath: modelPath.path)
        let originalTimestamp = attributes[.modificationDate] as! Date
        
        let downloadedMetadataFilenames = getRelativeFiles(url: metadataDestination)
        XCTAssertEqual(
            Set(downloadedMetadataFilenames),
            Set([
                ".cache/huggingface/download/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel.metadata",
            ])
        )
        
        // Corrupt model.metadata etag
        print("Testing corrupted etag.")
        let corruptedMetadataString = "a\nfc329090bfbb2570382c9af997cffd5f4b78b39b8aeca62076db69534e020108\n0\n"
        let metadataFile = metadataDestination.appendingPathComponent("llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel.metadata")
        try corruptedMetadataString.write(to: metadataFile, atomically: true, encoding: .utf8)
        
        let _ = try await hubApi.snapshot(from: repo, matching: "*.mlmodel") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        attributes = try FileManager.default.attributesOfItem(atPath: modelPath.path)
        let thirdDownloadTimestamp = attributes[.modificationDate] as! Date

        // File will not be downloaded again because this is an LFS file.
        // While downloading LFS files, we first check if local file ETag is the same as remote ETag.
        // If that's the case we just update the metadata and keep the local file.
        XCTAssertEqual(originalTimestamp, thirdDownloadTimestamp)
        
        let metadataString = try String(contentsOfFile: metadataFile.path)
        
        // Updated metadata file needs to have the correct commit hash, etag and timestamp.
        // This is being updated because the local etag (SHA256 checksum) matches the remote etag
        XCTAssertNotEqual(metadataString, corruptedMetadataString)
    }
    
    func testDownloadLargeFile() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination)
        var lastProgress: Progress? = nil
        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "*.mlmodel") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 1)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))

        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        XCTAssertEqual(Set(downloadedFilenames), Set(["llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel"]))
        
        let metadataDestination = downloadedTo.appending(component: ".cache/huggingface/download")
        let downloadedMetadataFilenames = getRelativeFiles(url: metadataDestination)
        XCTAssertEqual(
            Set(downloadedMetadataFilenames),
            Set([".cache/huggingface/download/llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel.metadata"])
        )
        
        let metadataFile = metadataDestination.appendingPathComponent("llama-2-7b-chat.mlpackage/Data/com.apple.CoreML/model.mlmodel.metadata")
        let metadataString = try String(contentsOfFile: metadataFile.path)
                
        let expected = "eaf97358a37d03fd48e5a87d15aff2e8423c1afb\nfc329090bfbb2570382c9af997cffd5f4b78b39b8aeca62076db69534e020107"
        XCTAssertTrue(metadataString.contains(expected))
    }
    
    func testDownloadSmolLargeFile() async throws {
        let hubApi = HubApi(downloadBase: downloadDestination)
        var lastProgress: Progress? = nil
        repo = "pcuenq/smol-lfs"
        let downloadedTo = try await hubApi.snapshot(from: repo, matching: "x.bin") { progress in
            print("Total Progress: \(progress.fractionCompleted)")
            print("Files Completed: \(progress.completedUnitCount) of \(progress.totalUnitCount)")
            lastProgress = progress
        }
        
        XCTAssertEqual(lastProgress?.fractionCompleted, 1)
        XCTAssertEqual(lastProgress?.completedUnitCount, 1)
        XCTAssertEqual(downloadedTo, downloadDestination.appending(path: "models/\(repo)"))

        let downloadedFilenames = getRelativeFiles(url: downloadDestination)
        XCTAssertEqual(Set(downloadedFilenames), Set(["x.bin"]))
        
        let metadataDestination = downloadedTo.appending(component: ".cache/huggingface/download")
        let downloadedMetadataFilenames = getRelativeFiles(url: metadataDestination)
        XCTAssertEqual(
            Set(downloadedMetadataFilenames),
            Set([".cache/huggingface/download/x.bin.metadata"])
        )
        
        let metadataFile = metadataDestination.appendingPathComponent("x.bin.metadata")
        let metadataString = try String(contentsOfFile: metadataFile.path)
        
        let expected = "77b984598d90af6143d73d5a2d6214b23eba7e27\n98ea6e4f216f2fb4b69fff9b3a44842c38686ca685f3f55dc48c5d3fb1107be4"
        XCTAssertTrue(metadataString.contains(expected))
    }
}
