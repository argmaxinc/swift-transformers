//
//  HubApi.swift
//
//
//  Created by Pedro Cuenca on 20231230.
//

import Foundation
import os
import CryptoKit

public struct HubApi {
    var downloadBase: URL
    var hfToken: String?
    var endpoint: String
    var useBackgroundSession: Bool

    public typealias RepoType = Hub.RepoType
    public typealias Repo = Hub.Repo
    
    public init(downloadBase: URL? = nil, hfToken: String? = nil, endpoint: String = "https://huggingface.co", useBackgroundSession: Bool = false) {
        self.hfToken = hfToken ?? ProcessInfo.processInfo.environment["HUGGING_FACE_HUB_TOKEN"]
        if let downloadBase {
            self.downloadBase = downloadBase
        } else {
            let documents = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
            self.downloadBase = documents.appending(component: "huggingface")
        }
        self.endpoint = endpoint
        self.useBackgroundSession = useBackgroundSession
    }
    
    public static let shared = HubApi()
    
    private static let logger = Logger()
}

/// File retrieval
public extension HubApi {
    /// Model data for parsed filenames
    struct Sibling: Codable {
        let rfilename: String
    }
    
    struct SiblingsResponse: Codable {
        let siblings: [Sibling]
    }
        
    /// Throws error if the response code is not 20X
    func httpGet(for url: URL) async throws -> (Data, HTTPURLResponse) {
        var request = URLRequest(url: url)
        if let hfToken = hfToken {
            request.setValue("Bearer \(hfToken)", forHTTPHeaderField: "Authorization")
        }
        let (data, response) = try await URLSession.shared.data(for: request)
        guard let response = response as? HTTPURLResponse else { throw Hub.HubClientError.unexpectedError }
        
        switch response.statusCode {
        case 200..<300: break
        case 400..<500: throw Hub.HubClientError.authorizationRequired
        default: throw Hub.HubClientError.httpStatusCode(response.statusCode)
        }

        return (data, response)
    }
    
    func httpHead(for url: URL) async throws -> (Data, HTTPURLResponse) {
        var request = URLRequest(url: url)
        request.httpMethod = "HEAD"
        if let hfToken = hfToken {
            request.setValue("Bearer \(hfToken)", forHTTPHeaderField: "Authorization")
        }
        request.setValue("identity", forHTTPHeaderField: "Accept-Encoding")
        let (data, response) = try await URLSession.shared.data(for: request)
        guard let response = response as? HTTPURLResponse else { throw Hub.HubClientError.unexpectedError }

        switch response.statusCode {
        case 200..<300: break
        case 400..<500: throw Hub.HubClientError.authorizationRequired
        default: throw Hub.HubClientError.httpStatusCode(response.statusCode)
        }
                
        return (data, response)
    }
    
    func getFilenames(from repo: Repo, matching globs: [String] = []) async throws -> [String] {
        // Read repo info and only parse "siblings"
        let url = URL(string: "\(endpoint)/api/\(repo.type)/\(repo.id)")!
        let (data, _) = try await httpGet(for: url)
        let response = try JSONDecoder().decode(SiblingsResponse.self, from: data)
        let filenames = response.siblings.map { $0.rfilename }
        guard globs.count > 0 else { return filenames }
        
        var selected: Set<String> = []
        for glob in globs {
            selected = selected.union(filenames.matching(glob: glob))
        }
        return Array(selected)
    }
    
    func getFilenames(from repoId: String, matching globs: [String] = []) async throws -> [String] {
        return try await getFilenames(from: Repo(id: repoId), matching: globs)
    }
    
    func getFilenames(from repo: Repo, matching glob: String) async throws -> [String] {
        return try await getFilenames(from: repo, matching: [glob])
    }
    
    func getFilenames(from repoId: String, matching glob: String) async throws -> [String] {
        return try await getFilenames(from: Repo(id: repoId), matching: [glob])
    }
}

/// Configuration loading helpers
public extension HubApi {
    /// Assumes the file has already been downloaded.
    /// `filename` is relative to the download base.
    func configuration(from filename: String, in repo: Repo) throws -> Config {
        let fileURL = localRepoLocation(repo).appending(path: filename)
        return try configuration(fileURL: fileURL)
    }
    
    /// Assumes the file is already present at local url.
    /// `fileURL` is a complete local file path for the given model
    func configuration(fileURL: URL) throws -> Config {
        let data = try Data(contentsOf: fileURL)
        let parsed = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dictionary = parsed as? [NSString: Any] else { throw Hub.HubClientError.parse }
        return Config(dictionary)
    }
}

/// Whoami
public extension HubApi {
    func whoami() async throws -> Config {
        guard hfToken != nil else { throw Hub.HubClientError.authorizationRequired }
        
        let url = URL(string: "\(endpoint)/api/whoami-v2")!
        let (data, _) = try await httpGet(for: url)

        let parsed = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dictionary = parsed as? [NSString: Any] else { throw Hub.HubClientError.parse }
        return Config(dictionary)
    }
}

/// Snaphsot download
public extension HubApi {
    func localRepoLocation(_ repo: Repo) -> URL {
        downloadBase.appending(component: repo.type.rawValue).appending(component: repo.id)
    }
    
    struct HubFileDownloader {
        let repo: Repo
        let repoDestination: URL
        let relativeFilename: String
        let hfToken: String?
        let endpoint: String?
        let backgroundSession: Bool

        var source: URL {
            // https://huggingface.co/coreml-projects/Llama-2-7b-chat-coreml/resolve/main/tokenizer.json?download=true
            var url = URL(string: endpoint ?? "https://huggingface.co")!
            if repo.type != .models {
                url = url.appending(component: repo.type.rawValue)
            }
            url = url.appending(path: repo.id)
            url = url.appending(path: "resolve/main") // TODO: revisions
            url = url.appending(path: relativeFilename)
            return url
        }
        
        var destination: URL {
            repoDestination.appending(path: relativeFilename)
        }
        
        var downloaded: Bool {
            FileManager.default.fileExists(atPath: destination.path)
        }
        
        func prepareDestination() throws {
            let directoryURL = destination.deletingLastPathComponent()
            try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true, attributes: nil)
        }

        // Note we go from Combine in Downloader to callback-based progress reporting
        // We'll probably need to support Combine as well to play well with Swift UI
        // (See for example PipelineLoader in swift-coreml-diffusers)
        @discardableResult
        func download(progressHandler: @escaping (Double) -> Void) async throws -> URL {
            guard !downloaded else { return destination }

            try prepareDestination()
            let downloader = Downloader(from: source, to: destination, using: hfToken, inBackground: backgroundSession)
            let downloadSubscriber = downloader.downloadState.sink { state in
                if case .downloading(let progress) = state {
                    progressHandler(progress)
                }
            }
            _ = try withExtendedLifetime(downloadSubscriber) {
                try downloader.waitUntilDone()
            }
            return destination
        }
    }

    @discardableResult
    func snapshot(
        from repo: Repo,
        config: SnapshotDownloadConfig = SnapshotDownloadConfig(),
        progressHandler: @escaping (Progress) -> Void = { _ in }
    ) async throws -> URL {
        // Get repo commit hash and list of files
        let (commitHash, repoFiles) = try await fetchRepoInfoFromServer(
            repo: repo,
            config: config
        )
        
        // Download files
        let progress = Progress(totalUnitCount: Int64(repoFiles.count))
        try await withThrowingTaskGroup(of: Void.self) { group in
            for filename in repoFiles {
                group.addTask {
                    let fileProgress = Progress(totalUnitCount: 100, parent: progress, pendingUnitCount: 1)
                    try await self.hubDownload(repo: repo, filename: filename, config: config)
                    fileProgress.completedUnitCount = 100
                    progressHandler(progress)
                }
            }
            try await group.waitForAll()
        }
        
        // Return local dir path if specified
        if let localDir = config.localDir {
            return localDir
        }
        
        let cacheDir = config.cacheDir ?? downloadBase
        return cacheDir
            .appending(component: repo.type.rawValue)
            .appending(component: repo.id)
            .appending(component: "snapshots")
            .appending(component: commitHash)
    }
    
    private func fetchRepoInfoFromServer(
        repo: Repo,
        config: SnapshotDownloadConfig
    ) async throws -> (String, [String]) {
        let filteredFiles = try await getFilenames(from: repo, matching: config.globs)
        
        let cacheDir = config.cacheDir ?? downloadBase
        let repoCache = cacheDir
            .appendingPathComponent(repo.type.rawValue)
            .appendingPathComponent(repo.id)
            
        let metadata = try await getFileMetadata(from: repo, matching: filteredFiles.first ?? "")
        guard let commitHash = metadata.first?.commitHash else {
            throw SnapshotError.invalidRevision
        }
        
        if config.revision != commitHash {
            let refPath = cacheDir
                .appending(component: "refs")
                .appending(component: config.revision)
            try? commitHash.write(to: refPath, atomically: true, encoding: .utf8)
        }
        
        return (commitHash, filteredFiles)
    }
    
    /// Download individual files from the hub
    @discardableResult
    private func hubDownload(
        repo: Repo,
        filename: String,
        config: SnapshotDownloadConfig
    ) async throws -> URL {
        // If local_dir specified, download there
        if let localDir = config.localDir {
            return try await downloadToLocalDir(
                repo: repo,
                filename: filename,
                config: config
            )
        }
        
        // Otherwise use cache dir
        return try await downloadToCache(
            repo: repo,
            filename: filename,
            config: config
        )
    }
    
    /// Download file to specified local directory
    private func downloadToLocalDir(
        repo: Repo,
        filename: String,
        config: SnapshotDownloadConfig
    ) async throws -> URL {
        let localDir = config.localDir!
        let paths = getLocalDownloadPaths(localDir: localDir, filename: filename)
        let localMetadata = try getLocalMetadata(localDir: localDir, filename: filename)
        
        // Check if file exists and is up to date
        if !config.forceDownload
            && isCommitHash(config.revision)
            && FileManager.default.fileExists(atPath: paths.filePath.path)
            && localMetadata != nil
            && localMetadata?.commitHash == config.revision {
            return paths.filePath
        }
        
        // Get remote metadata
        let (urlToDownload, etag, commitHash, expectedSize, headCallError) = try await getMetadataOrCatchError(
            repo: repo,
            filename: filename,
            config: config
        )
        
        if let error = headCallError {
            if !config.forceDownload && FileManager.default.fileExists(atPath: paths.filePath.path) {
                HubApi.logger.warning(
                    "Couldn't access the Hub to check for update but local file already exists. Defaulting to existing file. (error: \(error))"
                )
                return paths.filePath
            }
            try raiseOnHeadCallError(error, forceDownload: config.forceDownload, localFiles: config.localFiles)
        }
        
        guard let etag = etag,
              let commitHash = commitHash,
              let urlToDownload = urlToDownload,
              let expectedSize = expectedSize else {
            throw SnapshotError.invalidMetadata("Missing required metadata from server")
        }
        
        // Check if local file is up to date
        if !config.forceDownload && FileManager.default.fileExists(atPath: paths.filePath.path) {
            if let localMetadata = localMetadata, localMetadata.etag == etag {
                try writeDownloadMetadata(
                    localDir: localDir,
                    filename: filename,
                    commitHash: commitHash,
                    etag: etag
                )
                return paths.filePath
            }
            
            if localMetadata == nil && isSHA256(etag) {
                let fileHandle = try FileHandle(forReadingFrom: paths.filePath)
                let fileHash = try sha256(fileHandle: fileHandle)
                if fileHash == etag {
                    try writeDownloadMetadata(
                        localDir: localDir,
                        filename: filename,
                        commitHash: commitHash,
                        etag: etag
                    )
                    return paths.filePath
                }
            }
        }
        
        // Try to load from cache
        if !config.forceDownload {
            if let cachedPath = try loadFromCache(
                repo: repo,
                filename: filename,
                config: config
            ) {
               try await withFileLock(paths.lockPath) {
                   try FileManager.default.createDirectory(
                       at: paths.filePath.deletingLastPathComponent(),
                       withIntermediateDirectories: true
                   )
                   try FileManager.default.copyItem(at: cachedPath, to: paths.filePath)
               }
                try writeDownloadMetadata(
                    localDir: localDir,
                    filename: filename,
                    commitHash: commitHash,
                    etag: etag
                )
                return paths.filePath
            }
        }
        
        // Download file
        try await withFileLock(paths.lockPath) {
            try? FileManager.default.removeItem(at: paths.filePath)
            
            try await downloadToTmpAndMove(
                tempPath: paths.incompletePath(etag),
                destinationPath: paths.filePath,
                urlToDownload: urlToDownload,
                expectedSize: expectedSize,
                filename: filename,
                forceDownload: config.forceDownload,
                headers: config.headers
            )
        }
        
        try writeDownloadMetadata(
            localDir: localDir,
            filename: filename,
            commitHash: commitHash,
            etag: etag
        )
        
        return paths.filePath
    }
    
    /// Download file to cache directory
    private func downloadToCache(
        repo: Repo,
        filename: String,
        config: SnapshotDownloadConfig
    ) async throws -> URL {
        let cacheDir = config.cacheDir ?? downloadBase
        let revision = config.revision
        let locksDir = cacheDir.appendingPathComponent(".locks")
        let repoCache = cacheDir
            .appendingPathComponent(repo.type.rawValue)
            .appendingPathComponent(repo.id)
        
        // Check if file exists in cache
        if isCommitHash(revision) {
            let pointerPath = getPointerPath(repoCache, revision: revision, filename: filename)
            if FileManager.default.fileExists(atPath: pointerPath.path) && !config.forceDownload {
                return pointerPath
            }
        }
        
        // Get metadata from server
        let (urlToDownload, etag, commitHash, expectedSize, headCallError) = try await getMetadataOrCatchError(
            repo: repo,
            filename: filename,
            config: config
        )
        
        if let error = headCallError {
            if !config.forceDownload {
                var localCommitHash: String? = nil
                if isCommitHash(revision) {
                    localCommitHash = revision
                } else {
                    let refPath = repoCache
                        .appendingPathComponent("refs")
                        .appendingPathComponent(revision)
                    localCommitHash = try? String(contentsOf: refPath, encoding: .utf8)
                }
                
                if let hash = localCommitHash {
                    let pointerPath = getPointerPath(repoCache, revision: hash, filename: filename)
                    if FileManager.default.fileExists(atPath: pointerPath.path) && !config.forceDownload {
                        return pointerPath
                    }
                }
            }
            
            try raiseOnHeadCallError(error, forceDownload: config.forceDownload, localFiles: config.localFiles)
        }
        
        guard let etag = etag,
              let commitHash = commitHash,
              let urlToDownload = urlToDownload,
              let expectedSize = expectedSize else {
            throw SnapshotError.invalidMetadata("Required metadata missing from server")
        }
        
        let blobPath = repoCache
            .appendingPathComponent("blobs")
            .appendingPathComponent(etag)
        let pointerPath = getPointerPath(repoCache, revision: commitHash, filename: filename)
        
        try FileManager.default.createDirectory(
            at: blobPath.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try FileManager.default.createDirectory(
            at: pointerPath.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        
        if revision != commitHash {
            try cacheCommitHashForRevision(repoCache, revision: revision, commitHash: commitHash)
        }
        
        if !config.forceDownload {
            if FileManager.default.fileExists(atPath: pointerPath.path) {
                return pointerPath
            }
            
            if FileManager.default.fileExists(atPath: blobPath.path) {
                try createSymlink(from: blobPath, to: pointerPath, newBlob: false)
                return pointerPath
            }
        }
        
        let lockPath = locksDir
            .appendingPathComponent(repo.type.rawValue)
            .appendingPathComponent(repo.id)
            .appendingPathComponent("\(etag).lock")
        
        try FileManager.default.createDirectory(
            at: lockPath.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        
        try await withFileLock(lockPath) {
            try await downloadToTmpAndMove(
                tempPath: blobPath.appendingPathExtension("incomplete"),
                destinationPath: blobPath,
                urlToDownload: urlToDownload,
                expectedSize: expectedSize,
                filename: filename,
                forceDownload: config.forceDownload,
                headers: config.headers
            )
            
            if !FileManager.default.fileExists(atPath: pointerPath.path) {
                try createSymlink(from: blobPath, to: pointerPath, newBlob: true)
            }
        }
        
        return pointerPath
    }
    
    
    private func downloadToTmpAndMove(
        tempPath: URL,
        destinationPath: URL,
        urlToDownload: URL,
        expectedSize: Int?,
        filename: String,
        forceDownload: Bool = false,
        headers: [String: String]? = nil
    ) async throws {
        if FileManager.default.fileExists(atPath: destinationPath.path) && !forceDownload {
            return
        }
        
        let fileHandle = try FileHandle(forWritingTo: tempPath)
        defer { try? fileHandle.close() }
        
        let resumeSize = try fileHandle.seekToEnd()
        
        if let expectedSize = expectedSize {
            try checkDiskSpace(expectedSize, at: tempPath.deletingLastPathComponent())
            try checkDiskSpace(expectedSize, at: destinationPath.deletingLastPathComponent())
        }
        
        var request = URLRequest(url: urlToDownload)
        if let headers = headers {
            headers.forEach { request.setValue($0.value, forHTTPHeaderField: $0.key) }
        }
        if resumeSize > 0 {
            request.setValue("bytes=\(resumeSize)-", forHTTPHeaderField: "Range")
        }
        
        var retryCount = 5
        while true {
            do {
                try await downloadWithProgress(
                    request: request,
                    to: fileHandle,
                    resumeSize: resumeSize,
                    expectedSize: expectedSize,
                    filename: filename
                )
                break
            } catch {
                retryCount -= 1
                if retryCount <= 0 {
                    HubApi.logger.warning("Error while downloading from \(urlToDownload): \(error)\nMax retries exceeded.")
                    throw error
                }
                HubApi.logger.warning("Error while downloading from \(urlToDownload): \(error)\nTrying to resume download...")
                try await Task.sleep(nanoseconds: 1_000_000_000)
                await resetSessions()
            }
        }
        
        if let expectedSize = expectedSize {
            let actualSize = try fileHandle.seekToEnd()
            guard expectedSize == Int(actualSize) else {
                throw DownloadError.consistencyError(
                    "File should be size \(expectedSize) but has size \(actualSize) (\(filename))"
                )
            }
        }
        
        HubApi.logger.info("Download complete. Moving file to \(destinationPath)")
        try moveFile(from: tempPath, to: destinationPath)
    }
    
    private func downloadWithProgress(
        request: URLRequest,
        to fileHandle: FileHandle,
        resumeSize: UInt64,
        expectedSize: Int?,
        filename: String
    ) async throws {
        let (asyncBytes, response) = try await URLSession.shared.bytes(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse else {
            throw DownloadError.invalidResponse
        }
        
        if resumeSize > 0 && httpResponse.statusCode != 206 {
            throw DownloadError.resumeFailed
        }
        
        var downloadedBytes = resumeSize
        let totalBytes = expectedSize.map { UInt64($0) } ?? 0
        
        for try await byte in asyncBytes {
            try fileHandle.write(contentsOf: [byte])
            downloadedBytes += 1
            
            if totalBytes > 0 {
                let progress = Double(downloadedBytes) / Double(totalBytes)
                HubApi.logger.debug("Download progress for \(filename): \(Int(progress * 100))%")
            }
        }
    }
    
    private func checkDiskSpace(_ requiredBytes: Int, at path: URL) throws {
        let fileSystem = try FileManager.default.attributesOfFileSystem(forPath: path.path)
        guard let freeSpace = fileSystem[.systemFreeSize] as? Int else {
            HubApi.logger.warning("Could not determine free disk space")
            return
        }
        
        guard freeSpace >= requiredBytes else {
            throw DownloadError.insufficientDiskSpace(
                "Insufficient disk space. Need \(requiredBytes) bytes but only \(freeSpace) available"
            )
        }
    }
    
    private func moveFile(from src: URL, to dst: URL) throws {
        if FileManager.default.fileExists(atPath: dst.path) {
            try FileManager.default.removeItem(at: dst)
        }
        
        do {
            try FileManager.default.moveItem(at: src, to: dst)
        } catch {
            // If move fails, try copy
            try FileManager.default.copyItem(at: src, to: dst)
            try? FileManager.default.removeItem(at: src)
        }
    }
    
    private func getLocalDownloadPaths(localDir: URL, filename: String) -> LocalDownloadPaths {
        let filePath = localDir.appendingPathComponent(filename)
        let lockPath = filePath.appendingPathExtension("lock")
        return LocalDownloadPaths(filePath: filePath, lockPath: lockPath)
    }
    
    private func readDownloadMetadata(localDir: URL, filename: String) throws -> LocalFileMetadata {
        let metadataDir = localDir.appendingPathComponent(".cache/huggingface")
        let metadataPath = metadataDir.appendingPathComponent("\(filename).json")
        
        let data = try Data(contentsOf: metadataPath)
        return try JSONDecoder().decode(LocalFileMetadata.self, from: data)
    }
    
    private func writeDownloadMetadata(
        localDir: URL,
        filename: String,
        commitHash: String,
        etag: String
    ) throws {
        let metadataDir = localDir.appendingPathComponent(".cache/huggingface")
        try FileManager.default.createDirectory(
            at: metadataDir,
            withIntermediateDirectories: true
        )
        
        let metadataPath = metadataDir.appendingPathComponent("\(filename).json")
        let metadata = LocalFileMetadata(
            commitHash: commitHash,
            etag: etag,
            lastModified: Date(),
            size: nil
        )
        
        let encoder = JSONEncoder()
        encoder.outputFormatting = .prettyPrinted
        let data = try encoder.encode(metadata)
        try data.write(to: metadataPath)
    }
    
    private func writeLocalMetadata(
        _ metadata: LocalFileMetadata,
        at path: URL
    ) throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = .prettyPrinted
        let data = try encoder.encode(metadata)
        try data.write(to: path)
    }
    
    private func getPointerPath(_ repoCache: URL, revision: String, filename: String) -> URL {
        repoCache
            .appendingPathComponent("snapshots")
            .appendingPathComponent(revision)
            .appendingPathComponent(filename)
    }
    
    private func cacheCommitHashForRevision(_ repoCache: URL, revision: String, commitHash: String) throws {
        let refPath = repoCache
            .appendingPathComponent("refs")
            .appendingPathComponent(revision)
        try FileManager.default.createDirectory(
            at: refPath.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try commitHash.write(to: refPath, atomically: true, encoding: .utf8)
    }
    
    private func createSymlink(from blobPath: URL, to pointerPath: URL, newBlob: Bool) throws {
        try FileManager.default.createDirectory(
            at: pointerPath.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try FileManager.default.createSymbolicLink(
            at: pointerPath,
            withDestinationURL: blobPath
        )
    }
    
    private func loadFromCache(
        repo: Repo,
        filename: String,
        config: SnapshotDownloadConfig
    ) throws -> URL? {
        let cacheDir = config.cacheDir ?? downloadBase
        let repoCache = cacheDir
            .appendingPathComponent(repo.type.rawValue)
            .appendingPathComponent(repo.id)
        
        let snapshotPath = repoCache
            .appendingPathComponent("snapshots")
            .appendingPathComponent(config.revision)
            .appendingPathComponent(filename)
            
        if FileManager.default.fileExists(atPath: snapshotPath.path) {
            return snapshotPath
        }
        
        return nil
    }
    
    /// File locking utilities
    
    private func withFileLock<T>(_ path: URL, operation: () async throws -> T) async throws -> T {
        while FileManager.default.fileExists(atPath: path.path) {
            try await Task.sleep(nanoseconds: 100_000_000) // 100ms
        }
        FileManager.default.createFile(atPath: path.path, contents: nil)
        defer { try? FileManager.default.removeItem(at: path) }
        return try await operation()
    }
    
    /// Hash and pattern matching utilities
    
    private func sha256(fileHandle: FileHandle) throws -> String {
        var hasher = SHA256()
        let chunkSize = 1024 * 1024 // 1MB chunks
        
        while true {
            let data = fileHandle.readData(ofLength: chunkSize)
            if data.isEmpty { break }
            hasher.update(data: data)
        }
        
        return hasher.finalize().map { String(format: "%02x", $0) }.joined()
    }
    
    private func isCommitHash(_ revision: String) -> Bool {
        let commitHashPattern = "^[0-9a-f]{40}$"
        return (try? NSRegularExpression(pattern: commitHashPattern))?.matches(revision) ?? false
    }
    
    private func isSHA256(_ hash: String) -> Bool {
        let sha256Pattern = "^[0-9a-f]{64}$"
        return (try? NSRegularExpression(pattern: sha256Pattern))?.matches(hash) ?? false
    }
    
    /// Error handling utilities
    
    private func raiseOnHeadCallError(_ error: Error, forceDownload: Bool, localFiles: Bool) throws {
        if localFiles {
            throw SnapshotError.offlineMode(
                "Cannot find file in local cache and outgoing traffic has been disabled."
            )
        }
        
        switch error {
        case is RepositoryNotFoundError, is GatedRepoError:
            throw error
        default:
            throw SnapshotError.localEntryNotFound(
                "Cannot find file locally and failed to fetch from Hub: \(error)"
            )
        }
    }
    
    private func resetSessions() async {
        await URLSession.shared.reset()
    }
    
    /// Fetch commit hash and repo file names from the server
    private func fetchRepoInfoFromServer(
        repo: Repo,
        matching globs: [String] = [],
        revision: String = "main",
        storageFolder: URL
    ) async throws -> (String, [String]) {
        let fileNames = try await getFilenames(from: repo, matching: globs)
        
        let metadata = try await getFileMetadata(from: repo, matching: fileNames.first ?? "")
        guard let commitHash = metadata.first?.commitHash else {
            return ("", fileNames)
        }
        
        if revision != commitHash {
            let refPath = storageFolder
                .appending(component: "refs")
                .appending(component: revision)
            try? commitHash.write(to: refPath, atomically: true, encoding: .utf8)
        }
        
        return (commitHash, fileNames)
    }
    
    private func getLocalMetadata(localDir: URL, filename: String) throws -> LocalFileMetadata? {
        let metadataPath = localDir
            .appendingPathComponent(".cache/huggingface")
            .appendingPathComponent("\(filename).json")
        
        guard FileManager.default.fileExists(atPath: metadataPath.path) else {
            return nil
        }
        let data = try Data(contentsOf: metadataPath)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
      
        
        return try decoder.decode(LocalFileMetadata.self, from: data)
    }
    
    private func getMetadataOrCatchError(
        repo: Repo,
        filename: String,
        config: SnapshotDownloadConfig,
        repoCache: URL? = nil,
        relativeFilename: String? = nil
    ) async throws -> (
        urlToDownload: URL?,
        etag: String?,
        commitHash: String?,
        expectedSize: Int?,
        headCallError: Error?
    ) {
        if config.localFiles {
            return (nil, nil, nil, nil, SnapshotError.offlineMode("Local files only mode enabled"))
        }
        
        do {
            let metadata = try await getFileMetadata(from: repo, matching: filename).first
            
            // Cache non-existence of file if needed
            if metadata == nil,
               let repoCache = repoCache,
               let relativeFilename = relativeFilename {
                let noExistPath = repoCache
                    .appendingPathComponent(".no_exist")
                    .appendingPathComponent(config.revision)
                    .appendingPathComponent(relativeFilename)
                try? FileManager.default.createDirectory(
                    at: noExistPath.deletingLastPathComponent(),
                    withIntermediateDirectories: true
                )
                try? Data().write(to: noExistPath)
            }
            
            guard let metadata = metadata else {
                throw SnapshotError.fileNotFound(filename)
            }
            
            return (
                URL(string: metadata.location),
                metadata.etag,
                metadata.commitHash,
                metadata.size,
                nil
            )
            
        } catch {
            return (nil, nil, nil, nil, error)
        }
    }
    
    @discardableResult
    func snapshot(from repoId: String, config: SnapshotDownloadConfig, progressHandler: @escaping (Progress) -> Void = { _ in }) async throws -> URL {
        return try await snapshot(from: Repo(id: repoId), config: config, progressHandler: progressHandler)
    }
}

extension NSRegularExpression {
    func matches(_ string: String) -> Bool {
        let range = NSRange(location: 0, length: string.utf16.count)
        return firstMatch(in: string, options: [], range: range) != nil
    }
}

/// Metadata
public extension HubApi {
    struct LocalFileMetadata: Codable {
        let commitHash: String
        let etag: String
        let lastModified: Date?
        let size: Int?
        
        enum CodingKeys: String, CodingKey {
            case commitHash = "commit_hash"
            case etag
            case lastModified = "last_modified"
            case size
        }
    }
    
    /// A structure representing metadata for a remote file
    struct FileMetadata {
        /// The file's Git commit hash
        public let commitHash: String?
        
        /// Server-provided ETag for caching
        public let etag: String?
        
        /// Stringified URL location of the file
        public let location: String
        
        /// The file's size in bytes
        public let size: Int?
    }

    private func normalizeEtag(_ etag: String?) -> String? {
        guard let etag = etag else { return nil }
        return etag.trimmingPrefix("W/").trimmingCharacters(in: CharacterSet(charactersIn: "\""))
    }
    
    func getFileMetadata(url: URL) async throws -> FileMetadata {
        let (_, response) = try await httpHead(for: url)
        
        return FileMetadata(
            commitHash: response.value(forHTTPHeaderField: "X-Repo-Commit"),
            etag: normalizeEtag(
                (response.value(forHTTPHeaderField: "X-Linked-Etag")) ?? (response.value(forHTTPHeaderField: "Etag"))
            ),
            location: (response.value(forHTTPHeaderField: "Location")) ?? url.absoluteString,
            size: Int(response.value(forHTTPHeaderField: "X-Linked-Size") ?? response.value(forHTTPHeaderField: "Content-Length") ?? "")
        )
    }
    
    func getFileMetadata(from repo: Repo, matching globs: [String] = []) async throws -> [FileMetadata] {
        let files = try await getFilenames(from: repo, matching: globs)
        let url = URL(string: "\(endpoint)/\(repo.id)/resolve/main")! // TODO: revisions
        var selectedMetadata: Array<FileMetadata> = []
        for file in files {
            let fileURL = url.appending(path: file)
            selectedMetadata.append(try await getFileMetadata(url: fileURL))
        }
        return selectedMetadata
    }
    
    func getFileMetadata(from repoId: String, matching globs: [String] = []) async throws -> [FileMetadata] {
        return try await getFileMetadata(from: Repo(id: repoId), matching: globs)
    }
    
    func getFileMetadata(from repo: Repo, matching glob: String) async throws -> [FileMetadata] {
        return try await getFileMetadata(from: repo, matching: [glob])
    }
    
    func getFileMetadata(from repoId: String, matching glob: String) async throws -> [FileMetadata] {
        return try await getFileMetadata(from: Repo(id: repoId), matching: [glob])
    }
    
    func get(for url: URL) async throws -> (Data, HTTPURLResponse) {
        return try await httpGet(for: url)
    }
}

// Snapshot download
public extension HubApi {
    struct LocalDownloadPaths {
        let filePath: URL
        let lockPath: URL
        
        func incompletePath(_ etag: String?) -> URL {
            filePath.deletingLastPathComponent()
                .appendingPathComponent("._incomplete_\(etag ?? UUID().uuidString)")
        }
    }
    
    struct SnapshotDownloadConfig {
        let repoType: String?
        let revision: String
        let globs: [String]
        let cacheDir: URL?
        let localDir: URL?
        let forceDownload: Bool
        let localFiles: Bool
        let headers: [String: String]?
        
        public init(
            repoType: String? = nil,
            revision: String = "main",
            globs: [String] = [],
            cacheDir: URL? = nil,
            localDir: URL? = nil,
            forceDownload: Bool = false,
            localFiles: Bool = false,
            headers: [String: String]? = nil
        ) {
            self.repoType = repoType
            self.revision = revision
            self.globs = globs
            self.cacheDir = cacheDir
            self.localDir = localDir
            self.forceDownload = forceDownload
            self.localFiles = localFiles
            self.headers = headers
        }
    }

    enum SnapshotError: Error {
        case offlineMode(String)
        case localEntryNotFound(String)
        case invalidRevision
        case networkError(Error)
        case fileNotFound(String)
        case invalidMetadata(String)
        case gatedRepo
    }

    enum DownloadError: Error {
        case insufficientDiskSpace(String)
        case consistencyError(String)
        case invalidResponse
        case resumeFailed
    }
    
    enum RepositoryNotFoundError: Error {}
    
    enum GatedRepoError: Error {}
}

/// Stateless wrappers that use `HubApi` instances
public extension Hub {
    static func get(for url: URL) async throws -> (Data, HTTPURLResponse) {
        return try await HubApi.shared.get(for: url)
    }
    
    static func getFilenames(from repo: Hub.Repo, matching globs: [String] = []) async throws -> [String] {
        return try await HubApi.shared.getFilenames(from: repo, matching: globs)
    }
    
    static func getFilenames(from repoId: String, matching globs: [String] = []) async throws -> [String] {
        return try await HubApi.shared.getFilenames(from: Repo(id: repoId), matching: globs)
    }
    
    static func getFilenames(from repo: Repo, matching glob: String) async throws -> [String] {
        return try await HubApi.shared.getFilenames(from: repo, matching: glob)
    }
    
    static func getFilenames(from repoId: String, matching glob: String) async throws -> [String] {
        return try await HubApi.shared.getFilenames(from: Repo(id: repoId), matching: glob)
    }
    
//    static func snapshot(from repo: Repo, matching globs: [String] = [], progressHandler: @escaping (Progress) -> Void = { _ in }) async throws -> URL {
//        return try await HubApi.shared.snapshot(from: repo, config: HubApi.SnapshotDownloadConfig, progressHandler: progressHandler)
//    }
//    
//    static func snapshot(from repoId: String, matching globs: [String] = [], progressHandler: @escaping (Progress) -> Void = { _ in }) async throws -> URL {
//        return try await HubApi.shared.snapshot(from: Repo(id: repoId), matching: globs, progressHandler: progressHandler)
//    }
//    
//    static func snapshot(from repo: Repo, matching glob: String, progressHandler: @escaping (Progress) -> Void = { _ in }) async throws -> URL {
//        return try await HubApi.shared.snapshot(from: repo, matching: glob, progressHandler: progressHandler)
//    }
//    
//    static func snapshot(from repoId: String, matching glob: String, progressHandler: @escaping (Progress) -> Void = { _ in }) async throws -> URL {
//        return try await HubApi.shared.snapshot(from: Repo(id: repoId), matching: glob, progressHandler: progressHandler)
//    }
    
    static func whoami(token: String) async throws -> Config {
        return try await HubApi(hfToken: token).whoami()
    }
    
    static func getFileMetadata(fileURL: URL) async throws -> HubApi.FileMetadata {
        return try await HubApi.shared.getFileMetadata(url: fileURL)
    }
    
    static func getFileMetadata(from repo: Repo, matching globs: [String] = []) async throws -> [HubApi.FileMetadata] {
        return try await HubApi.shared.getFileMetadata(from: repo, matching: globs)
    }
    
    static func getFileMetadata(from repoId: String, matching globs: [String] = []) async throws -> [HubApi.FileMetadata] {
        return try await HubApi.shared.getFileMetadata(from: Repo(id: repoId), matching: globs)
    }
    
    static func getFileMetadata(from repo: Repo, matching glob: String) async throws -> [HubApi.FileMetadata] {
        return try await HubApi.shared.getFileMetadata(from: repo, matching: [glob])
    }
    
    static func getFileMetadata(from repoId: String, matching glob: String) async throws -> [HubApi.FileMetadata] {
        return try await HubApi.shared.getFileMetadata(from: Repo(id: repoId), matching: [glob])
    }
}

public extension [String] {
    func matching(glob: String) -> [String] {
        filter { fnmatch(glob, $0, 0) == 0 }
    }
}
