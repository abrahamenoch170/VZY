export class LastWriteWinsResolver {
    resolve(local, remote) {
        if (remote.timestamp > local.timestamp) {
            return remote;
        }
        if (remote.timestamp < local.timestamp) {
            return local;
        }
        if (remote.opId > local.opId) {
            return remote;
        }
        return local;
    }
}
//# sourceMappingURL=lww.js.map