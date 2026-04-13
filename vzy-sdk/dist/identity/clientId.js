const PREFIX = "vzy-client";
export function generateClientId() {
    const nativeCrypto = globalThis.crypto;
    if (nativeCrypto?.randomUUID) {
        return `${PREFIX}-${nativeCrypto.randomUUID()}`;
    }
    const fallback = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    return `${PREFIX}-${fallback}`;
}
export function generateOpId() {
    const nativeCrypto = globalThis.crypto;
    if (nativeCrypto?.randomUUID) {
        return nativeCrypto.randomUUID();
    }
    const n = () => Math.floor((1 + Math.random()) * 0x10000).toString(16).slice(1);
    return `${n()}${n()}-${n()}-4${n().slice(1)}-a${n().slice(1)}-${n()}${n()}${n()}`;
}
//# sourceMappingURL=clientId.js.map