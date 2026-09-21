import Darwin
import Foundation

/// Darwin 底层目录读取与文件描述符操作封装
public enum DirectoryReader {
    private static func childError(_ err: Int32, namePtr: UnsafePointer<CChar>) -> ScanError {
        let path = String(cString: namePtr)
        switch err {
        case ENOENT, ENOTDIR, ELOOP:
            return .changedDuringScan(path)
        case EACCES, EPERM:
            return .permissionDenied(path)
        default:
            return .ioError(err, "\(path): \(String(cString: strerror(err)))")
        }
    }
    /// 打开根目录
    public static func openRoot(path: String) throws -> (UnsafeMutablePointer<DIR>, FileIdentity) {
        var st = stat()
        if stat(path, &st) != 0 {
            let err = errno
            if err == ENOENT {
                throw ScanError.rootNotFound(path)
            } else if err == EACCES {
                throw ScanError.permissionDenied(path)
            } else {
                throw ScanError.ioError(err, String(cString: strerror(err)))
            }
        }

        if (st.st_mode & S_IFMT) != S_IFDIR {
            throw ScanError.rootNotDirectory(path)
        }

        guard let dir = opendir(path) else {
            let err = errno
            if err == EACCES {
                throw ScanError.permissionDenied(path)
            } else {
                throw ScanError.ioError(err, String(cString: strerror(err)))
            }
        }

        var opened = stat()
        guard fstat(dirfd(dir), &opened) == 0 else {
            let err = errno
            closedir(dir)
            throw ScanError.ioError(err, String(cString: strerror(err)))
        }
        guard (opened.st_mode & S_IFMT) == S_IFDIR else {
            closedir(dir)
            throw ScanError.rootNotDirectory(path)
        }

        return (dir, FileIdentity(device: opened.st_dev, inode: opened.st_ino))
    }

    /// 确认根路径在成功返回前仍命名本次扫描绑定的已打开根目录。
    public static func validateRootIdentity(path: String, identity: FileIdentity) throws {
        var current = stat()
        guard stat(path, &current) == 0,
              current.st_dev == identity.device,
              current.st_ino == identity.inode,
              (current.st_mode & S_IFMT) == S_IFDIR else {
            throw ScanError.changedDuringScan(path)
        }
    }

    /// 使用 parentFD 和单个 basename 打开子目录（不跟随符号链接）
    public static func openChild(
        parentFD: Int32,
        namePtr: UnsafePointer<CChar>
    ) throws -> UnsafeMutablePointer<DIR> {
        let childFD = openat(
            parentFD,
            namePtr,
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC
        )
        guard childFD >= 0 else {
            let err = errno
            throw childError(err, namePtr: namePtr)
        }

        guard let dir = fdopendir(childFD) else {
            let err = errno
            close(childFD)
            throw childError(err, namePtr: namePtr)
        }
        return dir
    }

    /// 读取已知或未知条目的类型（处理 DT_UNKNOWN 回退）
    public static func resolveType(
        dType: UInt8,
        parentFD: Int32,
        namePtr: UnsafePointer<CChar>
    ) throws -> EntryType {
        switch dType {
        case UInt8(DT_REG):
            return .file
        case UInt8(DT_DIR):
            return .directory
        case UInt8(DT_LNK):
            return .symbolicLink
        case UInt8(DT_UNKNOWN):
            var st = stat()
            guard fstatat(parentFD, namePtr, &st, AT_SYMLINK_NOFOLLOW) == 0 else {
                let err = errno
                throw childError(err, namePtr: namePtr)
            }
            let fmt = st.st_mode & S_IFMT
            if fmt == S_IFREG { return .file }
            if fmt == S_IFDIR { return .directory }
            if fmt == S_IFLNK { return .symbolicLink }
            return .other
        default:
            return .other
        }
    }

    /// 读取普通文件或节点的元数据（用于 .basic 模式）
    public static func readMetadata(
        parentFD: Int32,
        namePtr: UnsafePointer<CChar>,
        type: EntryType
    ) throws -> FileMetadata {
        var st = stat()
        guard fstatat(parentFD, namePtr, &st, AT_SYMLINK_NOFOLLOW) == 0 else {
            let err = errno
            throw childError(err, namePtr: namePtr)
        }

        let actualType: EntryType
        switch st.st_mode & S_IFMT {
        case S_IFREG: actualType = .file
        case S_IFDIR: actualType = .directory
        case S_IFLNK: actualType = .symbolicLink
        default: actualType = .other
        }
        guard actualType == type else {
            throw ScanError.changedDuringScan(String(cString: namePtr))
        }

        let identity = FileIdentity(device: st.st_dev, inode: st.st_ino)
        let mtime = FileTimestamp(
            seconds: Int64(st.st_mtimespec.tv_sec),
            nanoseconds: Int32(st.st_mtimespec.tv_nsec)
        )
        let ctime = FileTimestamp(
            seconds: Int64(st.st_ctimespec.tv_sec),
            nanoseconds: Int32(st.st_ctimespec.tv_nsec)
        )
        let size: Int64? = (type == .file) ? Int64(st.st_size) : nil

        return FileMetadata(
            identity: identity,
            modificationTime: mtime,
            changeTime: ctime,
            fileSize: size
        )
    }

    /// 提高当前进程的文件描述符软限制（避免大目录树并发扫描时受限于默认 256）
    public static func raiseFDLimit(to target: rlim_t = 10240) {
        var rlim = rlimit()
        if getrlimit(RLIMIT_NOFILE, &rlim) == 0 {
            let desired = min(target, rlim.rlim_max)
            if desired > rlim.rlim_cur {
                rlim.rlim_cur = desired
                _ = setrlimit(RLIMIT_NOFILE, &rlim)
            }
        }
    }
}
