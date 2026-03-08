/**
 * SessionStore — localStorage-backed session persistence for lightweight state.
 * IDBStore — IndexedDB-backed store for large data that exceeds localStorage limits.
 * Persists pipeline state, query library, settings across browser refreshes.
 */

const DB_NAME = 'dip_session';

class SessionStore {
    private store: Record<string, any> = {};
    private ready: boolean = false;

    async init(): Promise<void> {
        try {
            const raw = localStorage.getItem(DB_NAME);
            if (raw) {
                this.store = JSON.parse(raw);
            }
        } catch {
            this.store = {};
        }
        this.ready = true;
    }

    async get<T>(key: string, fallback: T): Promise<T> {
        if (!this.ready) await this.init();
        return key in this.store ? this.store[key] : fallback;
    }

    async set(key: string, value: any): Promise<void> {
        if (!this.ready) await this.init();
        this.store[key] = value;
        try {
            localStorage.setItem(DB_NAME, JSON.stringify(this.store));
        } catch {
            // Storage full — silently degrade
        }
    }

    async remove(key: string): Promise<void> {
        if (!this.ready) await this.init();
        delete this.store[key];
        localStorage.setItem(DB_NAME, JSON.stringify(this.store));
    }

    async keys(): Promise<string[]> {
        if (!this.ready) await this.init();
        return Object.keys(this.store);
    }

    async clear(): Promise<void> {
        this.store = {};
        localStorage.removeItem(DB_NAME);
    }
}

// Also try IndexedDB for large data
class IDBStore {
    private dbName = 'dip_idb';
    private storeName = 'session';
    private db: IDBDatabase | null = null;

    private open(): Promise<IDBDatabase> {
        if (this.db) return Promise.resolve(this.db);
        return new Promise((resolve, reject) => {
            const req = indexedDB.open(this.dbName, 1);
            req.onupgradeneeded = () => {
                req.result.createObjectStore(this.storeName);
            };
            req.onsuccess = () => {
                this.db = req.result;
                resolve(req.result);
            };
            req.onerror = () => reject(req.error);
        });
    }

    async get<T>(key: string): Promise<T | undefined> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readonly');
            const req = tx.objectStore(this.storeName).get(key);
            req.onsuccess = () => resolve(req.result);
            req.onerror = () => reject(req.error);
        });
    }

    async set(key: string, value: any): Promise<void> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readwrite');
            tx.objectStore(this.storeName).put(value, key);
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    }

    async remove(key: string): Promise<void> {
        const db = await this.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(this.storeName, 'readwrite');
            tx.objectStore(this.storeName).delete(key);
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    }
}

export const sessionStore = new SessionStore();
export const idbStore = new IDBStore();
