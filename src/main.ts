import { statSync, readFileSync, writeFileSync, unlinkSync, existsSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { Adapter, type AdapterOptions, getAbsoluteDefaultDataDir } from '@iobroker/adapter-core'; // Get common adapter utils

import DatabaseInfluxDB1x from './lib/DatabaseInfluxDB1x';
import DatabaseInfluxDB2x from './lib/DatabaseInfluxDB2x';
import type { Database, ValuesForInflux } from './lib/Database';
import * as Aggregate from './lib/aggregate';
import type {
    GetHistoryOptions,
    InfluxDBAdapterConfig,
    InfluxDbCustomConfig,
    InfluxDbCustomConfigTyped,
    IobDataEntry,
} from './types';
import { DockerManagerOfOwnContainers, type ContainerConfig } from '@iobroker/plugin-docker';
const dataDir = getAbsoluteDefaultDataDir();

const dockerDefaultToken = Buffer.from('iobroker86645638546565652656').toString('base64');

function isObject(it: any): boolean {
    // This is necessary because:
    // typeof null === 'object'
    // typeof [] === 'object'
    // [] instanceof Object === true
    return Object.prototype.toString.call(it) === '[object Object]';
}

function extractError(error: any): string {
    if (typeof error === 'string') {
        return error;
    }
    if (error instanceof Error) {
        if (Array.isArray((error as any).errors) && (error as any).errors.length) {
            return (error as any).errors.map((e: any) => (e.message ? e.message : JSON.stringify(e))).join(', ');
        }
        return error.message;
    }
    if (error === null || error === undefined) {
        return 'null';
    }

    return error.toString();
}

function sortByTs(
    a: { id?: string; val: number | string | boolean | null; ts: number },
    b: { id?: string; val: number | string | boolean | null; ts: number },
): 0 | 1 | -1 {
    const aTs = a.ts;
    const bTs = b.ts;
    return aTs < bTs ? -1 : aTs > bTs ? 1 : 0;
}

function parseBool(value: any, defaultValue?: boolean): boolean {
    if (value !== undefined && value !== null && value !== '') {
        return value === true || value === 'true' || value === 1 || value === '1';
    }
    return defaultValue || false;
}

function parseNumber(value: any, defaultValue?: number): number {
    if (typeof value === 'number') {
        return value;
    }
    if (typeof value === 'string') {
        const v = parseFloat(value);
        return isNaN(v) ? defaultValue || 0 : v;
    }
    return defaultValue || 0;
}

function parseNumberWithNull(value: any): number | null {
    if (typeof value === 'number') {
        return value;
    }
    if (typeof value === 'string') {
        const v = parseFloat(value);
        return isNaN(v) ? null : v;
    }
    return null;
}

function normalizeStateConfig(
    customConfig: InfluxDbCustomConfig,
    defaultConfig: InfluxDBAdapterConfig,
): InfluxDbCustomConfigTyped {
    // debounceTime and debounce compatibility handling
    if (!customConfig.blockTime && customConfig.blockTime !== '0' && customConfig.blockTime !== 0) {
        if (!customConfig.debounce && customConfig.debounce !== '0' && customConfig.debounce !== 0) {
            customConfig.blockTime = defaultConfig.blockTime || 0;
        } else {
            customConfig.blockTime = parseInt(customConfig.debounce as string, 10) || 0;
        }
    } else {
        customConfig.blockTime = parseInt(customConfig.blockTime as string, 10) || 0;
    }

    customConfig.debounceTime = parseNumber(customConfig.debounceTime, 0);
    customConfig.changesOnly = parseBool(customConfig.changesOnly);
    customConfig.ignoreZero = parseBool(customConfig.ignoreZero);

    // round
    if (customConfig.round !== null && customConfig.round !== undefined && customConfig.round !== '') {
        customConfig.round = parseInt(customConfig.round as string, 10);
        if (!isFinite(customConfig.round) || customConfig.round < 0) {
            customConfig.round = defaultConfig.round;
        } else {
            customConfig.round = Math.pow(10, parseInt(customConfig.round as unknown as string, 10));
        }
    } else {
        customConfig.round = defaultConfig.round;
    }

    customConfig.ignoreAboveNumber = parseNumberWithNull(customConfig.ignoreAboveNumber);
    customConfig.ignoreBelowNumber = parseNumberWithNull(customConfig.ignoreBelowNumber);
    if (customConfig.ignoreBelowNumber === null && parseBool(customConfig.ignoreBelowZero)) {
        customConfig.ignoreBelowNumber = 0;
    }

    customConfig.disableSkippedValueLogging = parseBool(
        customConfig.disableSkippedValueLogging,
        defaultConfig.disableSkippedValueLogging,
    );
    customConfig.enableDebugLogs = parseBool(customConfig.enableDebugLogs, defaultConfig.enableDebugLogs);
    customConfig.changesRelogInterval = parseNumber(
        customConfig.changesRelogInterval,
        defaultConfig.changesRelogInterval as number,
    );
    customConfig.changesMinDelta = parseNumber(customConfig.changesMinDelta, defaultConfig.changesMinDelta);

    customConfig.storageType ||= false;
    return customConfig as InfluxDbCustomConfigTyped;
}

interface SavedInfluxDbCustomConfig extends InfluxDbCustomConfigTyped {
    config: string;
    realId: string;
    storageTypeAdjustedInternally: boolean;
    relogTimeout: NodeJS.Timeout | null;
    state: ioBroker.State | null | undefined;
    skipped: ioBroker.State | null | undefined;
    timeout: NodeJS.Timeout | null;
    lastLogTime?: number;
}

export class InfluxDBAdapter extends Adapter {
    declare config: InfluxDBAdapterConfig;
    private _subscribeAll = false;
    // Mapping from AliasID to ioBroker ID
    private readonly _influxDPs: {
        [ioBrokerId: string]: SavedInfluxDbCustomConfig;
    } = {};
    private _client: Database | null = null;
    private _seriesBufferChecker: NodeJS.Timeout | null = null;
    private _seriesBufferCounter: number = 0;
    private _seriesBufferFlushPlanned = false;
    private _seriesBuffer: { [id: string]: ValuesForInflux[] } = {};
    private _conflictingPoints: { [id: string]: number } = {};
    private readonly _errorPoints: { [id: string]: number } = {};

    private readonly _tasksStart: string[] = [];
    private _connected: boolean | null = null;
    private _reconnectTimeout: NodeJS.Timeout | null = null;
    private _pingInterval: NodeJS.Timeout | null = null;
    private _finished = false;
    // mapping from ioBroker ID to Alias ID
    private readonly _aliasMap: { [ioBrokerId: string]: string } = {};
    // Per-instance cache file (must NOT be a module global, otherwise instances collide in compact mode)
    private _cacheFile = join(dataDir, 'influxdata.json');

    public constructor(options: Partial<AdapterOptions> = {}) {
        super({
            ...options,
            name: 'influxdb',
            ready: () => this.main(),
            message: (obj: ioBroker.Message) => this.processMessage(obj),
            stateChange: (id: string, state: ioBroker.State | null | undefined): void => {
                this.config.dbname ||= 'iobroker';
                id = this._aliasMap[id] ? this._aliasMap[id] : id;
                void this.pushHistory(id, state);
            },
            objectChange: (id: string, obj: ioBroker.Object | null | undefined): void => {
                this.config.dbname ||= 'iobroker';
                const formerAliasId = this._aliasMap[id] ? this._aliasMap[id] : id;
                if (
                    obj?.common?.custom?.[this.namespace] &&
                    typeof obj.common.custom[this.namespace] === 'object' &&
                    obj.common.custom[this.namespace].enabled
                ) {
                    const realId = id;
                    let checkForRemove = true;
                    if (obj.common.custom?.[this.namespace]?.aliasId) {
                        if (obj.common.custom[this.namespace].aliasId !== id) {
                            this._aliasMap[id] = obj.common.custom[this.namespace].aliasId;
                            this.log.debug(`Registered Alias: ${id} --> ${this._aliasMap[id]}`);
                            id = this._aliasMap[id];
                            checkForRemove = false;
                        } else {
                            this.log.warn(`Ignoring Alias-ID because identical to ID for ${id}`);
                            obj.common.custom[this.namespace].aliasId = '';
                        }
                    }
                    if (checkForRemove && this._aliasMap[id]) {
                        this.log.debug(`Removed Alias: ${id} !-> ${this._aliasMap[id]}`);
                        delete this._aliasMap[id];
                    }

                    // if not yet subscribed
                    if (!this._influxDPs[formerAliasId] && !this._subscribeAll) {
                        if (Object.keys(this._influxDPs).length >= 19) {
                            // unsubscribe all subscriptions and subscribe to all
                            for (const _id in this._influxDPs) {
                                this.unsubscribeForeignStates(this._influxDPs[_id].realId);
                            }
                            this._subscribeAll = true;
                            this.subscribeForeignStates('*');
                        } else {
                            this.subscribeForeignStates(realId);
                        }
                    }

                    const customSettings: InfluxDbCustomConfig = normalizeStateConfig(
                        obj.common.custom[this.namespace],
                        this.config,
                    );

                    if (
                        this._influxDPs[formerAliasId] &&
                        !this._influxDPs[formerAliasId].storageTypeAdjustedInternally &&
                        JSON.stringify(customSettings) === this._influxDPs[formerAliasId].config
                    ) {
                        if (customSettings.enableDebugLogs) {
                            this.log.debug(`Object ${id} unchanged. Ignore`);
                        }
                        return;
                    }

                    // relogTimeout
                    if (this._influxDPs[formerAliasId] && this._influxDPs[formerAliasId].relogTimeout) {
                        clearTimeout(this._influxDPs[formerAliasId].relogTimeout);
                        this._influxDPs[formerAliasId].relogTimeout = null;
                    }

                    const state = this._influxDPs[formerAliasId] ? this._influxDPs[formerAliasId].state : null;
                    const skipped = this._influxDPs[formerAliasId] ? this._influxDPs[formerAliasId].skipped : null;
                    const timeout = this._influxDPs[formerAliasId] ? this._influxDPs[formerAliasId].timeout : null;

                    this._influxDPs[id] = customSettings as SavedInfluxDbCustomConfig;
                    this._influxDPs[id].config = JSON.stringify(customSettings);
                    this._influxDPs[id].realId = realId;
                    this._influxDPs[id].state = state;
                    this._influxDPs[id].skipped = skipped;
                    this._influxDPs[id].timeout = timeout;

                    void this.writeInitialValue(realId, id);

                    this.log.info(`enabled logging of ${id}, Alias=${id !== realId}`);
                } else {
                    if (this._aliasMap[id]) {
                        this.log.debug(`Removed Alias: ${id} !-> ${this._aliasMap[id]}`);
                        delete this._aliasMap[id];
                    }

                    id = formerAliasId;

                    if (this._influxDPs[id]) {
                        const relogTimeout = this._influxDPs[id].relogTimeout;
                        if (relogTimeout) {
                            clearTimeout(relogTimeout);
                        }
                        const timeout = this._influxDPs[id].timeout;
                        if (timeout) {
                            clearTimeout(timeout);
                        }

                        delete this._influxDPs[id];
                        this.log.info(`disabled logging of ${id}`);
                        if (!this._subscribeAll) {
                            this.unsubscribeForeignStates(id);
                        }
                    }
                }
            },
            unload: (callback: () => void): void => {
                void this.finish(callback);
            },
        });
    }

    setConnected(isConnected: boolean): void {
        if (this._connected !== isConnected) {
            this._connected = isConnected;
            void this.setState('info.connection', this._connected, true, error =>
                // analyse if the state could be set (because of permissions)
                error
                    ? this.log.error(`Can not update this._connected state: ${error}`)
                    : this.log.debug(`connected set to ${this._connected}`),
            );
        }
    }

    reconnect(): void {
        this.setConnected(false);
        this.stopPing();
        if (!this._reconnectTimeout) {
            this._reconnectTimeout = setTimeout(() => {
                this._reconnectTimeout = null;
                void this.connect();
            }, this.config.reconnectInterval as number);
        }
    }

    startPing(): void {
        this._pingInterval ||= setInterval(() => this.ping(), this.config.pingInterval as number);
    }

    stopPing(): void {
        if (this._pingInterval) {
            clearInterval(this._pingInterval);
            this._pingInterval = null;
        }
    }

    async ping(): Promise<void> {
        if (this._client?.ping && this.config.pingserver !== false) {
            try {
                const hosts = await this._client.ping();
                if (!hosts.some(host => host.online)) {
                    this.reconnect();
                } else {
                    this.log.debug('PING OK');
                }
            } catch (error) {
                this.log.error(`Error during ping: ${extractError(error)}. Attempting reconnect.`);
                this.reconnect();
            }
        }
    }

    async connect(): Promise<void> {
        if (!this.config.path) {
            this.config.path = '';
        } else if (this.config.path.startsWith('/')) {
            this.config.path = this.config.path.substring(1);
        }

        this.log.info(
            `Connecting ${this.config.protocol}://${this.config.host}:${this.config.port}/${this.config.dbversion === '2.x' ? this.config.path || '' : ''} ...`,
        );

        this.config.validateSSL = this.config.validateSSL !== undefined ? !!this.config.validateSSL : true;

        this.config.seriesBufferMax = parseInt(this.config.seriesBufferMax as string, 10) || 0;

        this.log.info(`Influx DB Version used: ${this.config.dbversion}`);

        switch (this.config.dbversion) {
            case '2.x':
                // eslint-disable-next-line no-control-regex
                if (/[\x00-\x08\x0E-\x1F\x80-\xFF]/.test(this.config.token)) {
                    this.log.error('Token error: Please re-enter the token in Admin. Stopping');
                    return;
                }
                this._client = new DatabaseInfluxDB2x(
                    {
                        log: this.log,
                        host: this.config.host,
                        port: this.config.port,
                        protocol: this.config.protocol, // optional, default 'http'
                        database: this.config.dbname,
                        requestTimeout: this.config.requestTimeout as number,
                    },
                    {
                        path: this.config.path, // optional, default '/'
                        token: this.config.token,
                        organization: this.config.organization,
                        validateSSL: this.config.validateSSL,
                        useTags: this.config.usetags,
                    },
                );
                break;
            case '1.x':
            default:
                // eslint-disable-next-line no-control-regex
                if (/[\x00-\x08\x0E-\x1F\x80-\xFF]/.test(this.config.password)) {
                    return this.log.error('Password error: Please re-enter the password in Admin. Stopping');
                }

                this._client = new DatabaseInfluxDB1x(
                    {
                        log: this.log,
                        host: this.config.host,
                        port: this.config.port,
                        protocol: this.config.protocol, // optional, default 'http'
                        database: this.config.dbname,
                        requestTimeout: this.config.requestTimeout as number,
                    },
                    {
                        username: this.config.user,
                        password: decodeURIComponent(this.config.password || ''),
                    },
                );
                break;
        }

        if (this.config.pingserver === false) {
            this.log.info('Deactivated DB health checks (ping) via configuration');
        }
        if (!this._client) {
            this.log.error('No DB client created');
            return;
        }

        try {
            const dbNames: string[] = await this._client.getDatabaseNames();
            this.setConnected(true); // ??? to early, move down?
            if (!dbNames.includes(this.config.dbname)) {
                await this._client.createDatabase(this.config.dbname);
            }

            // Check and potentially update retention policy
            try {
                await this._client.applyRetentionPolicyToDB(this.config.dbname, this.config.retention as number);
            } catch (error) {
                // Ignore issues with creating/altering retention policy, as it might be due to insufficient permissions
                this.log.warn(extractError(error));
            }

            if (this.config.dbversion === '2.x') {
                // For 2.x the connection is finalized inside checkMetaDataStorageType (after the
                // tags/fields compatibility check), so it can abort the start on a conflict.
                await this.checkMetaDataStorageType();
            } else {
                // For 1.x there is no metadata storage type check, so finalize the connection here.
                // (Previously startPing/processStartValues/"Connected!" only ran for 2.x.)
                this.setConnected(true);
                await this.processStartValues();
                this.log.info('Connected!');
                this.startPing();
            }
        } catch (error) {
            this.log.error(extractError(error));
            this.reconnect();
        }
    }

    async checkMetaDataStorageType(): Promise<void> {
        try {
            const storageType = await this._client?.getMetaDataStorageType();
            this.log.debug(`Storage type for metadata found in DB: ${storageType}`);
            if ((storageType === 'tags' && !this.config.usetags) || (storageType === 'fields' && this.config.usetags)) {
                this.log.error(
                    `Cannot use ${this.config.usetags ? 'tags' : 'fields'} for metadata (q, ack, from) since ` +
                        `the selected DB already uses ${storageType} instead. Please change your adapter configuration, or choose a DB ` +
                        `that already uses ${this.config.usetags ? 'tags' : 'fields'}, or is empty.`,
                );
                this.setConnected(false);
                await this.finish();
            } else {
                this.setConnected(true);
                await this.processStartValues();
                this.log.info('Connected!');
                this.startPing();
            }
        } catch (error) {
            this.log.error(`Error checking for metadata storage type: ${extractError(error)}`);
        }
    }

    async processStartValues(): Promise<void> {
        for (const taskId of this._tasksStart) {
            if (taskId && this._influxDPs[taskId]?.changesOnly) {
                await this.pushHistory(taskId, this._influxDPs[taskId].state, true);
            }
        }
    }

    async getRetention(msg: ioBroker.Message): Promise<void> {
        this.log.debug('getRetention invoked, checking DB');
        try {
            const result = await this._client?.getRetentionPolicyForDB(this.config.dbname);
            this.sendTo(msg.from, msg.command, { result }, msg.callback);
        } catch (error) {
            this.sendTo(msg.from, msg.command, { error: extractError(error) }, msg.callback);
        }
    }

    getDockerConfigInflux(config: InfluxDBAdapterConfig): ContainerConfig {
        config.dockerInflux ||= {
            enabled: true,
        };
        // docker run -d -p 8086:8086 \
        //   -v $PWD/data:/var/lib/influxdb2 \
        //   -v $PWD/config:/etc/influxdb2 \
        //   -e DOCKER_INFLUXDB_INIT_MODE=setup \
        //   -e DOCKER_INFLUXDB_INIT_USERNAME=my-user \
        //   -e DOCKER_INFLUXDB_INIT_PASSWORD=my-password \
        //   -e DOCKER_INFLUXDB_INIT_ORG=my-org \
        //   -e DOCKER_INFLUXDB_INIT_BUCKET=my-bucket \
        //   influxdb:2
        config.dbversion = '2.x';
        config.dockerInflux.port = parseInt((config.dockerInflux.port as string) || '8086', 10) || 8086;
        config.protocol = 'http';
        const influxDockerConfig: ContainerConfig = {
            iobEnabled: true,
            iobStopOnUnload: true,
            removeOnExit: true,

            // influxdb image: https://hub.docker.com/_/influxdb. Only version 2 is supported
            image: 'influxdb:2',
            ports: [
                {
                    hostPort: config.dockerInflux.port,
                    containerPort: 8086,
                    hostIP: config.dockerInflux.bind || '127.0.0.1', // only localhost to disable authentication and https safely
                },
            ],
            mounts: [
                {
                    source: 'flux_data',
                    target: '/var/lib/influxdb2',
                    type: 'volume',
                    iobBackup: true,
                },
                {
                    source: 'flux_config',
                    target: '/etc/influxdb2',
                    type: 'volume',
                },
            ],
            networkMode: true, // take default name iob_influxdb_<instance>
            // influxdb v2 requires some environment variables to be set on first start
            environment: {
                DOCKER_INFLUXDB_INIT_MODE: 'setup',
                DOCKER_INFLUXDB_INIT_USERNAME: 'iobroker',
                DOCKER_INFLUXDB_INIT_PASSWORD: 'iobroker',
                DOCKER_INFLUXDB_INIT_BUCKET: 'iobroker',
                DOCKER_INFLUXDB_INIT_ORG: 'iobroker',
                DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: dockerDefaultToken,
            },
        };
        config.token = influxDockerConfig.environment!.DOCKER_INFLUXDB_INIT_ADMIN_TOKEN;
        config.organization = influxDockerConfig.environment!.DOCKER_INFLUXDB_INIT_ORG;

        return influxDockerConfig;
    }

    async testConnection(msg: ioBroker.Message): Promise<void> {
        this.log.debug(`testConnection msg-object: ${JSON.stringify(msg)}`);
        if (!msg?.message || !isObject(msg.message.config)) {
            return this.sendTo(msg.from, msg.command, { error: 'Invalid test configuration.' }, msg.callback);
        }
        const config: InfluxDBAdapterConfig = msg.message.config;
        config.port = parseInt(config.port as string, 10) || 0;
        config.requestTimeout = parseInt(config.requestTimeout as string) || 30000;

        let timeout: NodeJS.Timeout | null = null;
        try {
            timeout = setTimeout(() => {
                timeout = null;
                this.sendTo(msg.from, msg.command, { error: 'connect timeout' }, msg.callback);
            }, 5000);

            let lClient;
            this.log.debug(`TEST DB Version: ${config.dbversion}`);
            let dockerCreated = false;
            let dockerManager: DockerManagerOfOwnContainers | undefined;
            if (config.dockerInflux?.enabled) {
                // Start docker container if not running and then stop it
                const influxDockerConfig: ContainerConfig = this.getDockerConfigInflux(config);
                dockerManager = this.getPluginInstance('docker')?.getDockerManager();
                if (!dockerManager) {
                    dockerCreated = true;
                    influxDockerConfig.removeOnExit = true;
                    dockerManager = new DockerManagerOfOwnContainers(
                        {
                            logger: {
                                level: 'silly',
                                silly: this.log.silly.bind(this.log),
                                debug: this.log.debug.bind(this.log),
                                info: this.log.info.bind(this.log),
                                warn: this.log.warn.bind(this.log),
                                error: this.log.error.bind(this.log),
                            },
                            adapterDir: `${__dirname}/../`,
                            namespace: this.namespace,
                        },
                        [influxDockerConfig],
                    );
                }
            }
            config.dbname ||= 'iobroker';

            switch (config.dbversion) {
                case '2.x':
                    this.log.info('Connecting to InfluxDB 2');
                    lClient = new DatabaseInfluxDB2x(
                        {
                            log: this.log,
                            host: config.host,
                            port: config.port,
                            protocol: config.protocol, // optional, default 'http'
                            database: config.dbname,
                            requestTimeout: config.requestTimeout,
                        },
                        {
                            path: config.path, // optional, default '/'
                            token: config.token,
                            organization: config.organization,
                            validateSSL: config.validateSSL,
                            useTags: config.usetags,
                        },
                    );
                    break;
                default:
                case '1.x':
                    lClient = new DatabaseInfluxDB1x(
                        {
                            log: this.log,
                            host: config.host,
                            port: config.port, // optional, default 8086
                            protocol: config.protocol, // optional, default 'http'
                            database: config.dbname,
                            requestTimeout: config.requestTimeout,
                        },
                        {
                            username: config.user,
                            password: decodeURIComponent(config.password || ''),
                        },
                    );
                    break;
            }

            try {
                await lClient.getDatabaseNames();
                if (timeout) {
                    clearTimeout(timeout);
                    timeout = null;
                    return this.sendTo(msg.from, msg.command, { error: null }, msg.callback);
                }
            } catch (error) {
                if (timeout) {
                    clearTimeout(timeout);
                    timeout = null;
                    return this.sendTo(msg.from, msg.command, { error: extractError(error) }, msg.callback);
                }
            }
            if (dockerCreated && dockerManager) {
                try {
                    await dockerManager.destroy();
                } catch (e) {
                    this.log.error(`Cannot stop docker container: ${extractError(e)}`);
                }
            }
        } catch (error) {
            if (timeout) {
                clearTimeout(timeout);
                timeout = null;
            }
            if (extractError(error) === 'TypeError: undefined is not a function') {
                this.sendTo(
                    msg.from,
                    msg.command,
                    { error: 'Node.js DB driver could not be installed.' },
                    msg.callback,
                );
                return;
            }
            this.sendTo(msg.from, msg.command, { error: extractError(error) }, msg.callback);
        }
    }

    async destroyDB(msg: ioBroker.Message): Promise<void> {
        if (!this._client) {
            this.sendTo(msg.from, msg.command, { error: 'Not connected' }, msg.callback);
            return;
        }
        try {
            await this._client.dropDatabase(this.config.dbname);

            // tests need a clear database at start
            if (!msg.message?.noRestart) {
                this.sendTo(msg.from, msg.command, { error: null }, msg.callback);
                // restart adapter
                setTimeout(() => {
                    void this.getForeignObject(`system.adapter.${this.namespace}`, (error, obj) => {
                        if (!error) {
                            if (obj) {
                                void this.setForeignObject(obj._id, obj);
                            }
                        } else {
                            this.log.error(`Cannot read object "system.adapter.${this.namespace}": ${error}`);
                            void this.stop?.();
                        }
                    });
                }, 2000);
            } else {
                // create db
                await this._client.createDatabase(this.config.dbname);
                try {
                    await this._client.applyRetentionPolicyToDB(this.config.dbname, this.config.retention as number);
                } catch (error) {
                    // Ignore issues with creating/altering retention policy, as it might be due to insufficient permissions
                    this.log.warn(extractError(error));
                }

                if (this.config.dbversion === '2.x') {
                    await this.checkMetaDataStorageType();
                }
                this.sendTo(msg.from, msg.command, { error: null }, msg.callback);
            }
        } catch (error) {
            this.sendTo(msg.from, msg.command, { error: extractError(error) }, msg.callback);
        }
    }

    async processMessage(msg: ioBroker.Message): Promise<void> {
        this.log.debug(`Incoming message ${msg.command} from ${msg.from}`);
        this.config.dbname ||= 'iobroker';
        try {
            if (msg.command === 'features') {
                // Currently the supported features are identical for InfluxDB 1.x and 2.x
                this.sendTo(
                    msg.from,
                    msg.command,
                    { supportedFeatures: ['update', 'delete', 'deleteRange', 'deleteAll', 'storeState'] },
                    msg.callback,
                );
            } else if (msg.command === 'update') {
                await this.updateState(msg);
            } else if (msg.command === 'delete') {
                await this.deleteStateFromDB(msg);
            } else if (msg.command === 'deleteAll') {
                await this.deleteStateAll(msg);
            } else if (msg.command === 'deleteRange') {
                await this.deleteStateFromDB(msg);
            } else if (msg.command === 'storeState') {
                await this.storeState(msg);
            } else if (msg.command === 'getHistory') {
                if (this.config.dbversion === '1.x') {
                    await this.getHistoryV1(msg);
                } else {
                    await this.getHistoryV2(msg);
                }
            } else if (msg.command === 'test') {
                await this.testConnection(msg);
            } else if (msg.command === 'destroy') {
                await this.destroyDB(msg);
            } else if (msg.command === 'query') {
                switch (this.config.dbversion) {
                    case '2.x':
                        // Influx 2.x uses Flux instead of InfluxQL,
                        // so for multiple statements there is no delimiter by default, so we introduce ;
                        await this.multiQuery(msg);
                        break;
                    case '1.x':
                    default:
                        await this.query(msg);
                        break;
                }
            } else if (msg.command === 'getConflictingPoints') {
                this.getConflictingPoints(msg);
            } else if (msg.command === 'resetConflictingPoints') {
                this.resetConflictingPoints(msg);
            } else if (msg.command === 'flushBuffer') {
                const id = msg.message ? msg.message.id : undefined;
                this.log.debug(`Flushing buffer for ${id || 'all'}`);
                try {
                    await this.storeBufferedSeries(id);
                    if (msg.callback) {
                        this.sendTo(msg.from, msg.command, { error: null }, msg.callback);
                    }
                } catch (error) {
                    if (msg.callback) {
                        this.sendTo(msg.from, msg.command, { error: extractError(error) }, msg.callback);
                    }
                }
            } else if (msg.command === 'enableHistory') {
                this.enableHistory(msg);
            } else if (msg.command === 'disableHistory') {
                this.disableHistory(msg);
            } else if (msg.command === 'getEnabledDPs') {
                this.getEnabledDPs(msg);
            } else if (msg.command === 'stopInstance') {
                void this.finish(() => {
                    if (msg.callback) {
                        this.sendTo(msg.from, msg.command, 'stopped', msg.callback);
                        setTimeout(() => (this.terminate ? this.terminate() : process.exit()), 200);
                    }
                });
            } else if (msg.command === 'getRetention') {
                await this.getRetention(msg);
            }
        } catch (error) {
            this.log.error(`Cannot process message ${msg.command}: ${extractError(error)}`);
            if (msg.callback) {
                this.sendTo(msg.from, msg.command, { error: extractError(error) }, msg.callback);
            }
        }
    }

    getConflictingPoints(msg: ioBroker.Message): void {
        return this.sendTo(msg.from, msg.command, { conflictingPoints: this._conflictingPoints }, msg.callback);
    }

    resetConflictingPoints(msg: ioBroker.Message): void {
        const resultMsg = { reset: true, conflictingPoints: this._conflictingPoints };
        this._conflictingPoints = {};
        return this.sendTo(msg.from, msg.command, resultMsg, msg.callback);
    }

    async main(): Promise<void> {
        this.config.dbname ||= 'iobroker';
        this.config.port = parseInt(this.config.port as string, 10) || 0;
        if (this.config.relogLastValueOnStart === undefined) {
            this.config.relogLastValueOnStart = true;
        } else if (this.config.relogLastValueOnStart === 'false') {
            this.config.relogLastValueOnStart = false;
        }
        // set default history if not yet set
        const obj = await this.getForeignObjectAsync('system.config');
        if (obj?.common && !obj.common.defaultHistory) {
            obj.common.defaultHistory = this.namespace;
            this.setForeignObject('system.config', obj, error => {
                if (error) {
                    this.log.error(`Cannot set default history instance: ${error}`);
                } else {
                    this.log.info(`Set default history instance to "${this.namespace}"`);
                }
            });
        }

        this.setConnected(false);

        this.config.reconnectInterval = parseInt(this.config.reconnectInterval as string, 10) || 10000;
        this.config.pingInterval = parseInt(this.config.pingInterval as string, 10) || 15000;

        if (this.config.round !== null && this.config.round !== undefined && this.config.round !== '') {
            this.config.round = parseInt(this.config.round as string, 10);
            if (!isFinite(this.config.round) || this.config.round < 0) {
                this.config.round = null;
                this.log.info(`Invalid round value: ${this.config.round} - ignore, do not round values`);
            } else {
                this.config.round = Math.pow(10, this.config.round);
            }
        } else {
            this.config.round = null;
        }
        this.config.changesRelogInterval = parseNumber(this.config.changesRelogInterval, 0);
        if (this.config.changesRelogInterval !== null && this.config.changesRelogInterval !== undefined) {
            if (this.config.changesRelogInterval > 2147483) {
                this.log.warn(
                    'changesRelogInterval was configured too high. Please correct this in the settings. Set to 2147483 seconds (max. value).',
                );
                this.config.changesRelogInterval = 2147483;
            }
        }
        this.config.seriesBufferFlushInterval = parseInt(this.config.seriesBufferFlushInterval as string, 10) || 600;
        this.config.requestTimeout = parseInt(this.config.requestTimeout as string, 10) || 30000;
        this.config.changesMinDelta = parseNumber(this.config.changesMinDelta.toString().replace(/,/g, '.'), 0);

        if (this.config.blockTime !== null && this.config.blockTime !== undefined) {
            this.config.blockTime = parseInt(this.config.blockTime as string, 10) || 0;
        } else {
            if (this.config.debounce !== null && this.config.debounce !== undefined) {
                this.config.debounce = parseInt(this.config.debounce as string, 10) || 0;
            } else {
                this.config.debounce = 0;
            }
        }
        this.config.debounceTime = parseNumber(this.config.debounceTime, 0);

        this.config.retention = parseInt(this.config.retention as string, 10) || 0;
        if (this.config.retention === -1) {
            // Custom timeframe
            this.config.retention = (parseInt(this.config.customRetentionDuration as string, 10) || 0) * 24 * 60 * 60;
        }

        if (this.instance !== 0) {
            this._cacheFile = this._cacheFile.replace(/\.json$/, `_${this.instance}.json`);
        }
        // analyse if by the last stop the values were cached into file
        try {
            if (statSync(this._cacheFile).isFile()) {
                const fileContent = readFileSync(this._cacheFile, 'utf-8');
                const tempData = JSON.parse(fileContent, (key, value) => (key === 'time' ? new Date(value) : value));

                if (tempData.seriesBufferCounter) {
                    this._seriesBufferCounter = tempData.seriesBufferCounter;
                }
                if (tempData.seriesBuffer) {
                    this._seriesBuffer = tempData.seriesBuffer;
                }
                if (tempData.conflictingPoints) {
                    this._conflictingPoints = tempData.conflictingPoints;
                }
                this.log.info(
                    `Buffer initialized with data for ${this._seriesBufferCounter} points and ${Object.keys(this._conflictingPoints).length} conflicts from last exit`,
                );
                unlinkSync(this._cacheFile);
            }
        } catch {
            this.log.info('No stored data from last exit found');
        }

        // read all custom settings
        const doc = await this.getObjectViewAsync('system', 'custom', {});

        if (doc?.rows) {
            const l = doc.rows.length;
            for (let i = 0; i < l; i++) {
                if (doc.rows[i].value) {
                    const item: {
                        id: string;
                        value: {
                            [key: `${string}.${number}`]: InfluxDbCustomConfigTyped;
                        };
                    } = doc.rows[i];
                    let id = item.id;
                    const realId = id;
                    if (!item.value[this.namespace]?.enabled) {
                        continue;
                    }
                    if (item.value[this.namespace]?.aliasId) {
                        this._aliasMap[id] = item.value[this.namespace].aliasId;
                        this.log.debug(`Found Alias: ${id} --> ${this._aliasMap[id]}`);
                        id = this._aliasMap[id];
                    }
                    this._influxDPs[id] = normalizeStateConfig(
                        item.value[this.namespace],
                        this.config,
                    ) as SavedInfluxDbCustomConfig;
                    this._influxDPs[id].config = JSON.stringify(item.value[this.namespace]);
                    this.log.debug(`enabled logging of ${id}, Alias=${id !== realId} points now activated`);

                    this._influxDPs[id].realId = realId;
                    await this.writeInitialValue(realId, id);
                }
            }
        }

        // If we have less than 20 datapoints, subscribe individually, else subscribe to all
        if (Object.keys(this._influxDPs).length < 20) {
            this.log.info(`subscribing to ${Object.keys(this._influxDPs).length} datapoints`);
            for (const _id in this._influxDPs) {
                if (Object.prototype.hasOwnProperty.call(this._influxDPs, _id)) {
                    this.subscribeForeignStates(this._influxDPs[_id].realId);
                }
            }
        } else {
            this.log.debug(
                `subscribing to all datapoints as we have ${Object.keys(this._influxDPs).length} datapoints to log`,
            );
            this._subscribeAll = true;
            this.subscribeForeignStates('*');
        }

        this.subscribeForeignObjects('*');

        if (this.config.dockerInflux?.enabled) {
            this.config.dbversion = '2.x';
            this.config.dockerInflux.port = parseInt((this.config.dockerInflux.port as string) || '8086', 10) || 8086;
            this.config.protocol = 'http';
            this.config.token = dockerDefaultToken;
            this.config.organization = 'iobroker';
        }

        if (this.config.dockerInflux?.enabled && this.config.dockerGrafana?.enabled) {
            this.prepareDockerConfigGrafana(this.config);
            // Inform docker plugin about grafana provisioning folder is ready
            this.getPluginInstance('docker')?.instanceIsReady();
        }

        void this.connect();

        if (this._client) {
            // store all buffered data every x seconds to not lost the data
            this._seriesBufferChecker = setInterval(() => {
                this._seriesBufferFlushPlanned = true;
                void this.storeBufferedSeries().catch(e =>
                    this.log.error(`Cannot store buffered series: ${extractError(e)}`),
                );
            }, this.config.seriesBufferFlushInterval * 1000);
        }
    }

    prepareDockerConfigGrafana(config: InfluxDBAdapterConfig): void {
        // ensure that the folders exist
        const provisioningFolder = join(__dirname, 'grafana-provisioning', 'datasources');
        if (!existsSync(provisioningFolder)) {
            mkdirSync(provisioningFolder, { recursive: true });
        }
        writeFileSync(
            join(__dirname, 'grafana-provisioning', 'datasources', 'datasource.yml'),
            `apiVersion: 1

datasources:
  - name: InfluxDB
    type: influxdb
    access: proxy
    url: http://iob_${this.namespace.replace(/[-.]/g, '_')}:${config.dockerInflux?.port || 8086}
    jsonData:
      version: Flux
      organization: iobroker
      defaultBucket: iobroker
    secureJsonData:
      token: ${dockerDefaultToken}  
    isDefault: true
`,
        );
    }

    async writeInitialValue(realId: string, id: string): Promise<void> {
        const state = await this.getForeignStateAsync(realId);
        if (state && this._influxDPs[id]) {
            state.from = `system.adapter.${this.namespace}`;
            this._influxDPs[id].state = state;
            if (this.config.relogLastValueOnStart) {
                this._tasksStart.push(id);
                if (this._tasksStart.length === 1 && this._connected) {
                    await this.processStartValues();
                }
            }
        }
    }

    async pushHistory(id: string, state: ioBroker.State | null | undefined, timerRelog?: boolean): Promise<void> {
        timerRelog ||= false;
        // Push into InfluxDB
        if (this._influxDPs[id]) {
            const settings = this._influxDPs[id];

            if (!settings || !state) {
                return;
            }

            if (state && state.val === undefined) {
                this.log.warn(`state value undefined received for ${id} which is not allowed. Ignoring.`);
                return;
            }

            if (typeof state.val === 'string' && settings.storageType !== 'String') {
                if (isFinite(state.val as unknown as number)) {
                    state.val = parseFloat(state.val);
                }
            }

            settings.enableDebugLogs &&
                this.log.debug(
                    `new value received for ${id} (storageType ${settings.storageType}), new-value=${state.val}, ts=${state.ts}, relog=${timerRelog}`,
                );

            let ignoreDebounce = false;

            if (settings.changesRelogInterval !== null && settings.changesRelogInterval !== undefined) {
                if (settings.changesRelogInterval > 2147483) {
                    this.log.warn(
                        `changesRelogInterval was configured too high. Please correct this in the settings of '${id}'. Set to 2147483 seconds (max. value).`,
                    );
                    settings.changesRelogInterval = 2147483;
                }
            }

            if (!timerRelog) {
                const valueUnstable = !!this._influxDPs[id].timeout;
                // When a debounce timer runs and the value is the same as the last one, ignore it
                if (this._influxDPs[id].timeout && state.ts !== state.lc) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value not changed debounce ${id}, value=${state.val}, ts=${state.ts}, debounce timer keeps running`,
                        );
                    return;
                } else if (this._influxDPs[id].timeout) {
                    // if value changed, clear timer
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value changed during debounce time ${id}, value=${state.val}, ts=${state.ts}, debounce timer restarted`,
                        );
                    clearTimeout(this._influxDPs[id].timeout);
                    this._influxDPs[id].timeout = null;
                }

                if (
                    !valueUnstable &&
                    settings.blockTime &&
                    this._influxDPs[id].state &&
                    this._influxDPs[id].state.ts + settings.blockTime > state.ts
                ) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value ignored blockTime ${id}, value=${state.val}, ts=${state.ts}, lastState.ts=${this._influxDPs[id].state.ts}, blockTime=${settings.blockTime}`,
                        );
                    return;
                }

                if (settings.ignoreZero && (state.val === undefined || state.val === null || state.val === 0)) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value ignore because zero or null ${id}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    return;
                }
                if (
                    typeof settings.ignoreBelowNumber === 'number' &&
                    typeof state.val === 'number' &&
                    state.val < settings.ignoreBelowNumber
                ) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value ignored because below ${settings.ignoreBelowNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    return;
                }
                if (
                    typeof settings.ignoreAboveNumber === 'number' &&
                    typeof state.val === 'number' &&
                    state.val > settings.ignoreAboveNumber
                ) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `value ignored because above ${settings.ignoreAboveNumber} for ${id}, new-value=${state.val}, ts=${state.ts}`,
                        );
                    return;
                }

                if (this._influxDPs[id].state && settings.changesOnly) {
                    if (settings.changesRelogInterval === 0) {
                        if ((this._influxDPs[id].state.val !== null || state.val === null) && state.ts !== state.lc) {
                            // remember new timestamp
                            if (!valueUnstable && !settings.disableSkippedValueLogging) {
                                this._influxDPs[id].skipped = state;
                            }
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `value not changed ${id}, last-value=${this._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                            return;
                        }
                    } else if (this._influxDPs[id].lastLogTime) {
                        if (
                            (this._influxDPs[id].state.val !== null || state.val === null) &&
                            state.ts !== state.lc &&
                            Math.abs(this._influxDPs[id].lastLogTime - state.ts) < settings.changesRelogInterval * 1000
                        ) {
                            // remember new timestamp
                            if (!valueUnstable && !settings.disableSkippedValueLogging) {
                                this._influxDPs[id].skipped = state;
                            }
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `value not changed ${id}, last-value=${this._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                            return;
                        }
                        if (state.ts !== state.lc) {
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `value-not-changed-relog ${id}, value=${state.val}, lastLogTime=${this._influxDPs[id].lastLogTime}, ts=${state.ts}`,
                                );
                            ignoreDebounce = true;
                        }
                    }
                    if (typeof state.val === 'number') {
                        if (
                            this._influxDPs[id].state.val !== null &&
                            settings.changesMinDelta !== 0 &&
                            Math.abs((this._influxDPs[id].state.val as number) - state.val) < settings.changesMinDelta
                        ) {
                            if (!valueUnstable && !settings.disableSkippedValueLogging) {
                                this._influxDPs[id].skipped = state;
                            }
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `Min-Delta not reached ${id}, last-value=${this._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                            return;
                        } else if (settings.changesMinDelta !== 0) {
                            settings.enableDebugLogs &&
                                this.log.debug(
                                    `Min-Delta reached ${id}, last-value=${this._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                                );
                        }
                    } else {
                        settings.enableDebugLogs &&
                            this.log.debug(
                                `Min-Delta ignored because no number ${id}, last-value=${this._influxDPs[id].state.val}, new-value=${state.val}, ts=${state.ts}`,
                            );
                    }
                }
            }

            if (this._influxDPs[id].relogTimeout) {
                clearTimeout(this._influxDPs[id].relogTimeout);
                this._influxDPs[id].relogTimeout = null;
            }
            if (timerRelog) {
                state = Object.assign({}, state);
                state.ts = Date.now();
                state.from = `system.adapter.${this.namespace}`;
                settings.enableDebugLogs &&
                    this.log.debug(
                        `timed-relog ${id}, value=${state.val}, lastLogTime=${this._influxDPs[id].lastLogTime}, ts=${state.ts}`,
                    );
                ignoreDebounce = true;
            } else {
                if (settings.changesOnly && this._influxDPs[id].skipped) {
                    settings.enableDebugLogs &&
                        this.log.debug(
                            `Skipped value logged ${id}, value=${this._influxDPs[id].skipped.val}, ts=${this._influxDPs[id].skipped.ts}`,
                        );
                    try {
                        await this.pushHelper(id, this._influxDPs[id].skipped);
                    } catch (e) {
                        this.log.warn(`Cannot push skipped value: ${e}`);
                    }
                    this._influxDPs[id].skipped = null;
                }
                if (
                    this._influxDPs[id].state &&
                    ((this._influxDPs[id].state.val === null && state.val !== null) ||
                        (this._influxDPs[id].state.val !== null && state.val === null))
                ) {
                    ignoreDebounce = true;
                } else if (!this._influxDPs[id].state && state.val === null) {
                    ignoreDebounce = true;
                }
            }

            if (settings.debounceTime && !ignoreDebounce && !timerRelog) {
                // Discard changes in de-bounce time to store last stable value
                this._influxDPs[id].timeout && clearTimeout(this._influxDPs[id].timeout);
                this._influxDPs[id].timeout = setTimeout(
                    (id, state) => {
                        this._influxDPs[id].timeout = null;
                        this._influxDPs[id].state = state;
                        this._influxDPs[id].lastLogTime = state.ts;
                        settings.enableDebugLogs &&
                            this.log.debug(
                                `Value logged ${id}, value=${this._influxDPs[id].state.val}, ts=${this._influxDPs[id].state.ts}`,
                            );
                        void this.pushHelper(id).catch(e => this.log.warn(e));
                        if (settings.changesOnly && settings.changesRelogInterval > 0) {
                            this._influxDPs[id].relogTimeout = setTimeout(
                                _id => this.reLogHelper(_id),
                                settings.changesRelogInterval * 1000,
                                id,
                            );
                        }
                    },
                    settings.debounceTime,
                    id,
                    state,
                );
            } else {
                if (!timerRelog) {
                    this._influxDPs[id].state = state;
                }
                this._influxDPs[id].lastLogTime = state.ts;

                if (settings.enableDebugLogs) {
                    this.log.debug(
                        `Value logged ${id}, value=${this._influxDPs[id].state!.val}, ts=${this._influxDPs[id].state!.ts}`,
                    );
                }
                void this.pushHelper(id, state).catch(e => this.log.warn(e));
                if (settings.changesOnly && settings.changesRelogInterval > 0) {
                    this._influxDPs[id].relogTimeout = setTimeout(
                        _id => this.reLogHelper(_id),
                        settings.changesRelogInterval * 1000,
                        id,
                    );
                }
            }
        }
    }

    async reLogHelper(_id: string): Promise<void> {
        if (!this._influxDPs[_id]) {
            this.log.info(`non-existing id ${_id}`);
            return;
        }
        this._influxDPs[_id].relogTimeout = null;
        if (this._influxDPs[_id].skipped) {
            await this.pushHistory(_id, this._influxDPs[_id].skipped, true);
        } else if (this._influxDPs[_id].state) {
            await this.pushHistory(_id, this._influxDPs[_id].state, true);
        } else {
            try {
                const state = await this.getForeignStateAsync(this._influxDPs[_id].realId);
                if (!state) {
                    this.log.info(
                        `init timed Relog: disable relog because state not set so far for ${_id}: ${JSON.stringify(state)}`,
                    );
                } else if (this._influxDPs[_id]) {
                    this.log.debug(
                        `init timed Relog: getState ${_id}:  Value=${state.val}, ack=${state.ack}, ts=${state.ts}, lc=${state.lc}`,
                    );
                    this._influxDPs[_id].state = state;
                    await this.pushHistory(_id, this._influxDPs[_id].state, true);
                }
            } catch (error) {
                this.log.info(`init timed Relog: can not get State for ${_id}: ${extractError(error)}`);
            }
        }
    }

    async pushHelper(_id: string, state?: ioBroker.State | null): Promise<void> {
        if (!state && !this._influxDPs[_id]?.state) {
            throw new Error(`No state to log for ID ${_id}`);
        }
        state ||= this._influxDPs[_id]?.state;

        if (!state) {
            throw new Error(`No state to log for ID ${_id}`);
        }

        // Important: We allow also to store "unknown" states, so use fallback here
        const _settings = this._influxDPs[_id] || ({} as SavedInfluxDbCustomConfig);

        if (state.val === null) {
            // InfluxDB can not handle null values
            throw new Error(`null value for ${_id} can not be handled`);
        }
        if (typeof state.val === 'number' && !isFinite(state.val)) {
            // InfluxDB can not handle Infinite values
            throw new Error(`Non Finite value ${state.val} for ${_id} can not be handled`);
        }

        if (state.val !== null && (typeof state.val === 'object' || typeof state.val === 'undefined')) {
            state.val = JSON.stringify(state.val);
        }

        _settings.enableDebugLogs &&
            this.log.debug(`Datatype ${_id}: Currently: ${typeof state.val}, StorageType: ${_settings.storageType}`);
        if (typeof state.val === 'string' && _settings.storageType !== 'String') {
            _settings.enableDebugLogs && this.log.debug(`Do Automatic Datatype conversion for ${_id}`);
            if (isFinite(state.val as unknown as number)) {
                state.val = parseFloat(state.val);
            } else if (state.val === 'true') {
                state.val = true;
            } else if (state.val === 'false') {
                state.val = false;
            }
        }
        if (_settings.storageType === 'String' && typeof state.val !== 'string') {
            state.val = state.val === null ? 'null' : state.val.toString();
        } else if (_settings.storageType === 'Number' && typeof state.val !== 'number') {
            if (typeof state.val === 'boolean') {
                state.val = state.val ? 1 : 0;
            } else {
                this.log.info(`Do not store value "${state.val}" for ${_id} because no number`);
                throw new Error(`do not store value for ${_id} because no number`);
            }
        } else if (_settings.storageType === 'Boolean' && typeof state.val !== 'boolean') {
            state.val = !!state.val;
        }
        await this.pushValueIntoDB(_id, state, false);
    }

    async pushValueIntoDB(id: string, state: ioBroker.State, directWrite: boolean): Promise<void> {
        if (!this._client) {
            this.log.warn('No connection to DB');
            throw new Error('No connection to DB');
        }

        if (state.val === null || state.val === undefined) {
            throw new Error('InfluxDB can not handle null/non-existing values');
        } // InfluxDB can not handle null/non-existing values
        if (typeof state.val === 'number' && !isFinite(state.val)) {
            throw new Error(`InfluxDB can not handle non finite values like ${state.val}`);
        }

        // Ensure the timestamp is an integer (ms). Fall back to current time for invalid timestamps.
        state.ts = parseInt(state.ts as unknown as string, 10);
        if (!isFinite(state.ts)) {
            state.ts = Date.now();
        }

        if (typeof state.val === 'object') {
            state.val = JSON.stringify(state.val);
        }

        //this.log.debug('write value ' + state.val + ' for ' + id);
        const influxFields: ValuesForInflux = {
            value: state.val,
            time: state.ts,
            from: state.from || '',
            q: state.q || 0,
            ack: !!state.ack,
        };

        if (
            (this._conflictingPoints[id] || this.config.seriesBufferMax === 0 || directWrite) &&
            this._connected &&
            this._client.getHostsAvailable() > 0
        ) {
            if (this.config.seriesBufferMax !== 0) {
                this.log.debug(
                    `Direct writePoint("${id} - ${influxFields.value} / ${influxFields.time.toLocaleString()}")`,
                );
            }
            await this.writeOnePointForID(id, influxFields, true);
        } else {
            await this.addPointToSeriesBuffer(id, influxFields);
        }
    }

    async addPointToSeriesBuffer(id: string, stateObj: ValuesForInflux): Promise<void> {
        if (
            (this._conflictingPoints[id] || this.config.seriesBufferMax === 0) &&
            this._connected &&
            this._client?.getHostsAvailable()
        ) {
            if (this.config.seriesBufferMax !== 0) {
                this.log.debug(`Direct writePoint("${id} - ${stateObj.value} / ${stateObj.time.toLocaleString()}")`);
            }
            await this.writeOnePointForID(id, stateObj, true);
            // The point was written directly, so it must NOT also be added to the buffer,
            // otherwise it would be written a second time on the next flush.
            // (On write failure writeOnePointForID re-adds the point to the buffer itself.)
            return;
        }

        this._seriesBuffer[id] ||= [];
        this._seriesBuffer[id].push(stateObj);
        this._seriesBufferCounter++;
        if (
            this._seriesBufferCounter > (this.config.seriesBufferMax as number) &&
            this._connected &&
            this._client?.getHostsAvailable() &&
            !this._seriesBufferFlushPlanned
        ) {
            // flush out
            this._seriesBufferFlushPlanned = true;
            await this.storeBufferedSeries();
        }
    }

    async storeBufferedSeries(id?: string | null): Promise<number> {
        if (id && !this._seriesBuffer[id]?.length) {
            return 0;
        }
        if (!Object.keys(this._seriesBuffer).length) {
            return 0;
        }

        if (!this._client?.getHostsAvailable()) {
            this.setConnected(false);
            this.log.info('Currently no hosts available, try later');
            this._seriesBufferFlushPlanned = false;
            throw new Error('Currently no hosts available, try later');
        }
        if (!this._connected) {
            this.log.info('Not connected to InfluxDB, try later');
            this._seriesBufferFlushPlanned = false;
            throw new Error('Not connected to InfluxDB, try later');
        }

        if (id) {
            const idSeries = this._seriesBuffer[id];
            this._seriesBuffer[id] = [];
            this.log.debug(`Store ${idSeries.length} buffered influxDB history points for ${id}`);
            this._seriesBufferCounter -= idSeries.length;
            await this.writeSeriesPerID(id, idSeries);
            return idSeries.length;
        }

        if (this._seriesBufferChecker) {
            clearInterval(this._seriesBufferChecker);
            this._seriesBufferChecker = null;
        }

        const result = this._seriesBufferCounter;
        this.log.info(`Store ${result} buffered influxDB history points`);

        // Snapshot the buffer and reset it BEFORE awaiting the write. Otherwise points that are
        // pushed via addPointToSeriesBuffer() while the (async) write is in flight would be
        // discarded by the reset afterwards (data loss). On write failure the write helpers
        // re-add the failed points into the (now fresh) this._seriesBuffer, where they correctly
        // merge with the newly arrived points.
        const currentBuffer = this._seriesBuffer;
        this._seriesBuffer = {};
        this._seriesBufferCounter = 0;

        if (result > 15000) {
            // if we have too many data points in buffer, we better writer them per id
            this.log.info(`Too many data points (${result}) to write at once; write per ID`);
            await this.writeAllSeriesPerID(currentBuffer);
        } else {
            await this.writeAllSeriesAtOnce(currentBuffer);
        }
        this._seriesBufferFlushPlanned = false;
        this._seriesBufferChecker = setInterval(
            () => this.storeBufferedSeries(),
            (this.config.seriesBufferFlushInterval as number) * 1000,
        );
        return result;
    }

    async writeAllSeriesAtOnce(series: { [id: string]: ValuesForInflux[] }): Promise<void> {
        try {
            await this._client?.writeSeries(series);
            this.setConnected(true);
        } catch (error) {
            this.log.warn(`Error on writeSeries: ${extractError(error)}`);
            if (!this._client?.getHostsAvailable()) {
                this.setConnected(false);
                this.log.info('Host not available, move all points back in the Buffer');
                // error caused InfluxDB this._client to remove the host from available for now
                Object.keys(series).forEach(id => {
                    if (series[id].length) {
                        this._seriesBuffer[id] = this._seriesBuffer[id] || [];
                        this._seriesBufferCounter += series[id].length;
                        series[id].forEach(s => this._seriesBuffer[id].push(s));
                    }
                });
                this.reconnect();
            } else {
                const errorText = extractError(error);
                if (errorText && errorText.includes('partial write') && !errorText.includes('field type conflict')) {
                    this.log.warn('All possible data points were written, others can not really be corrected');
                } else {
                    this.log.info(
                        `Try to write ${Object.keys(series).length} Points separate to find the conflicting id`,
                    );
                    // fallback and send data per id to find out problematic id!
                    await this.writeAllSeriesPerID(series);
                }
            }
        }
    }

    async writeAllSeriesPerID(series: { [id: string]: ValuesForInflux[] }): Promise<void> {
        const idList = Object.keys(series);
        if (!idList.length) {
            return;
        }
        for (const id of idList) {
            await this.writeSeriesPerID(id, series[id]);
        }
    }

    async writeSeriesPerID(seriesId: string, points: ValuesForInflux[]): Promise<void> {
        if (!points.length) {
            return;
        }
        this.log.debug(`writePoints ${points.length} for ${seriesId} at once`);

        do {
            const pointsToSend = points.splice(0, 15000);
            if (points.length) {
                // We still have some left
                this.log.info(
                    `Too many dataPoints (${pointsToSend.length + points.length}) for "${seriesId}" to write at once; split in 15.000 batches`,
                );
            }
            try {
                await this._client?.writePoints(seriesId, pointsToSend);
                this.setConnected(true);
            } catch (error) {
                this.log.warn(`Error on writePoints for ${seriesId}: ${extractError(error)}`);
                if (
                    !this._client?.getHostsAvailable() ||
                    (error.message && (error.message === 'timeout' || error.message.includes('timed out')))
                ) {
                    this.log.info('Host not available, move all points back in the Buffer');
                    // error caused InfluxDB this._client to remove the host from available for now
                    this._seriesBuffer[seriesId] ||= [];

                    for (let i = 0; i < pointsToSend.length; i++) {
                        this._seriesBuffer[seriesId].push(pointsToSend[i]);
                        this._seriesBufferCounter++;
                    }
                    for (let i = 0; i < points.length; i++) {
                        this._seriesBuffer[seriesId].push(points[i]);
                        this._seriesBufferCounter++;
                    }
                    this.reconnect();
                    return;
                }
                this.log.warn(`Try to write ${pointsToSend.length} Points separate to find the conflicting one`);
                // we found the conflicting id
                await this.writePointsForID(seriesId, pointsToSend);
            }
        } while (points.length);
    }

    async writePointsForID(seriesId: string, points: ValuesForInflux[]): Promise<void> {
        if (!points.length) {
            return;
        }
        this.log.debug(`writePoint ${points.length} for ${seriesId} separate`);

        for (const point of points) {
            await this.writeOnePointForID(seriesId, point, false);
        }
    }

    async writeOnePointForID(pointId: string, point: ValuesForInflux, directWrite?: boolean): Promise<void> {
        directWrite ||= false;

        if (!this._connected) {
            await this.addPointToSeriesBuffer(pointId, point);
            return;
        }

        try {
            await this._client?.writePoint(pointId, point);
            this.setConnected(true);
            // Successful write: reset the error counter so a previously flaky point can recover
            // instead of accumulating errors across unrelated transient failures.
            if (this._errorPoints[pointId]) {
                this._errorPoints[pointId] = 0;
            }
        } catch (error) {
            this.log.warn(`Error on writePoint("${JSON.stringify(point)}): ${extractError(error)}"`);
            const errorText = extractError(error);
            if (!this._client?.getHostsAvailable() || errorText.includes('timeout')) {
                this.reconnect();
                await this.addPointToSeriesBuffer(pointId, point);
            } else if (errorText.includes('field type conflict')) {
                // retry write after type correction for some easy cases
                let retry = false;
                let adjustType = false;
                if (this._influxDPs[pointId] && !this._influxDPs[pointId].storageType) {
                    let convertDirection = '';
                    if (
                        errorText.includes('is type bool, already exists as type float') ||
                        errorText.includes('is type boolean, already exists as type float')
                    ) {
                        convertDirection = 'bool -> float';
                        if (point.value === true) {
                            point.value = 1;
                            retry = true;
                        } else if (point.value === false) {
                            point.value = 0;
                            retry = true;
                        }
                        adjustType = true;
                        this._influxDPs[pointId].storageType = 'Number';
                        this._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    } else if (
                        errorText.includes('is type float, already exists as type bool') ||
                        errorText.includes('is type float64, already exists as type bool')
                    ) {
                        convertDirection = 'float -> bool';
                        if (point.value === 1) {
                            point.value = true;
                            retry = true;
                        } else if (point.value === 0) {
                            point.value = false;
                            retry = true;
                        }
                        adjustType = true;
                        this._influxDPs[pointId].storageType = 'Boolean';
                        this._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    } else if (errorText.includes(', already exists as type string')) {
                        point.value = point.value.toString();
                        retry = true;
                        adjustType = true;
                        this._influxDPs[pointId].storageType = 'String';
                        this._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    } else if (errorText.includes('is type string, already exists as type float')) {
                        if (isFinite(point.value as unknown as number)) {
                            point.value = parseFloat(point.value as string);
                            retry = true;
                        }
                        adjustType = true;
                        this._influxDPs[pointId].storageType = 'Number';
                        this._influxDPs[pointId].storageTypeAdjustedInternally = true;
                    }
                    if (retry) {
                        this.log.info(
                            `Try to convert ${convertDirection} and re-write for ${pointId} and set storageType to ${this._influxDPs[pointId].storageType}`,
                        );
                        await this.writeOnePointForID(pointId, point, true);
                    }
                    if (adjustType) {
                        const obj: ioBroker.StateObject = {} as ioBroker.StateObject;
                        obj.common = {} as ioBroker.StateCommon;
                        obj.common.custom = {};
                        obj.common.custom[this.namespace] = {};
                        obj.common.custom[this.namespace].storageType = this._influxDPs[pointId].storageType;

                        this.extendForeignObject(pointId, obj, error => {
                            if (error) {
                                this.log.error(
                                    `error updating history config for ${pointId} to pin datatype: ${error}`,
                                );
                            } else {
                                this.log.info(`changed history configuration to pin detected datatype for ${pointId}`);
                            }
                        });
                    }
                }
                if (!directWrite || !retry) {
                    // remember this as a pot. conflicting points and write synchronous
                    this._conflictingPoints[pointId] = 1;
                    this.log.warn(
                        `Add ${pointId} to conflicting Points (${Object.keys(this._conflictingPoints).length} now)`,
                    );
                }
            } else {
                if (!this._errorPoints[pointId]) {
                    this._errorPoints[pointId] = 1;
                } else {
                    this._errorPoints[pointId]++;
                }
                if (this._errorPoints[pointId] < 10) {
                    // re-add that point to buffer to try to write again
                    this.log.info(
                        `Add point that had error for ${pointId} to buffer again, error-count=${this._errorPoints[pointId]}`,
                    );
                    await this.addPointToSeriesBuffer(pointId, point);
                } else {
                    this.log.info(
                        `Discard point that had error for ${pointId}, error-count=${this._errorPoints[pointId]}`,
                    );
                    this._errorPoints[pointId] = 0;
                }
            }
        }
    }

    writeFileBufferToDisk(): void {
        // write buffered values into cache file to process it by next start
        if (this._seriesBufferCounter) {
            const fileData: {
                seriesBufferCounter: number;
                seriesBuffer: { [id: string]: ValuesForInflux[] };
                conflictingPoints: { [id: string]: number };
            } = {
                seriesBufferCounter: this._seriesBufferCounter,
                seriesBuffer: this._seriesBuffer,
                conflictingPoints: this._conflictingPoints,
            };

            try {
                writeFileSync(this._cacheFile, JSON.stringify(fileData), 'utf-8');
                this.log.warn(
                    `Store data for ${fileData.seriesBufferCounter} points and ${Object.keys(fileData.conflictingPoints).length} conflicts`,
                );
            } catch (error) {
                this.log.warn(`Could not save non-stored data to file: ${extractError(error)}`);
            }
        }
        this._seriesBufferCounter = 0;
    }

    async _delete(
        id: string,
        state: {
            start?: number;
            end?: number;
            ts?: number;
        },
    ): Promise<void> {
        if (!this._connected) {
            throw new Error('not connected');
        }

        if (this.config.dbversion === '1.x') {
            let query;
            if (state.ts) {
                query = `DELETE FROM "${id}" WHERE time = '${new Date(state.ts).toISOString()}'`;
            } else if (state.start) {
                query = `DELETE FROM "${id}" WHERE time >= '${new Date(state.start).toISOString()}'${state.end ? ` AND time <= '${new Date(state.end).toISOString()}'` : ''}`;
            } else if (state.end) {
                query = `DELETE FROM "${id}" WHERE time <= '${new Date(state.end).toISOString()}'`;
            } else {
                query = `DELETE FROM "${id}" WHERE time >= '2000-01-01T00:00:00.000Z'`; // delete all
            }

            try {
                await this._client?.query(query);
                this.setConnected(true);
            } catch (error) {
                this.log.warn(`Error on delete("${query}): ${extractError(error)}"`);
                throw error;
            }
        } else if (this.config.dbversion === '2.x') {
            let start;
            let stop;
            if (state.ts) {
                start = state.ts;
                stop = state.ts;
            } else if (state.start) {
                // deletes from "state.start" until ...
                start = state.start;
                if (state.end) {
                    stop = state.end; // ... "state.end"
                } else {
                    stop = new Date(); // ... now
                }
            } else if (state.end) {
                // deletes from 01.01.1970 until "state.end"
                start = new Date(0);
                stop = state.end;
            } else {
                // deletes from 01.01.1970 until now
                start = new Date(0);
                stop = new Date();
            }

            try {
                await this._client?.deleteData(
                    start,
                    stop,
                    this.config.organization,
                    this.config.dbname,
                    `_measurement="${id}"`,
                );
                if (this._client) {
                    this.setConnected(true);
                }
            } catch (error) {
                this.log.warn(`Error on delete("${extractError(error)}"`);
                throw error;
            }
        } else {
            throw new Error('not implemented');
        }
    }

    async deleteStateFromDB(msg: ioBroker.Message): Promise<void> {
        if (!msg.message) {
            this.log.error('deleteStateFromDB called with invalid data');
            this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
            return;
        }
        let id;
        if (Array.isArray(msg.message)) {
            this.log.debug(`deleteStateFromDB ${msg.message.length} items`);

            for (let i = 0; i < msg.message.length; i++) {
                id = this._aliasMap[msg.message[i].id] ? this._aliasMap[msg.message[i].id] : msg.message[i].id;

                // {id: 'blabla', ts: 892}
                if (msg.message[i].ts) {
                    await this._delete(id, { ts: msg.message[i].ts });
                } else if (msg.message[i].start) {
                    if (typeof msg.message[i].start === 'string') {
                        msg.message[i].start = new Date(msg.message[i].start).getTime();
                    }
                    if (typeof msg.message[i].end === 'string') {
                        msg.message[i].end = new Date(msg.message[i].end).getTime();
                    }
                    await this._delete(id, { start: msg.message[i].start, end: msg.message[i].end || Date.now() });
                } else if (
                    typeof msg.message[i].state === 'object' &&
                    msg.message[i].state &&
                    msg.message[i].state.ts
                ) {
                    await this._delete(id, { ts: msg.message[i].state.ts });
                } else if (
                    typeof msg.message[i].state === 'object' &&
                    msg.message[i].state &&
                    msg.message[i].state.start
                ) {
                    if (typeof msg.message[i].state.start === 'string') {
                        msg.message[i].state.start = new Date(msg.message[i].state.start).getTime();
                    }
                    if (typeof msg.message[i].state.end === 'string') {
                        msg.message[i].state.end = new Date(msg.message[i].state.end).getTime();
                    }
                    await this._delete(id, {
                        start: msg.message[i].state.start,
                        end: msg.message[i].state.end || Date.now(),
                    });
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
                }
            }
        } else if (msg.message.state && Array.isArray(msg.message.state)) {
            this.log.debug(`deleteStateFromDB ${msg.message.state.length} items`);
            id = this._aliasMap[msg.message.id] ? this._aliasMap[msg.message.id] : msg.message.id;

            for (let j = 0; j < msg.message.state.length; j++) {
                if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                    if (msg.message.state[j].ts) {
                        await this._delete(id, { ts: msg.message.state[j].ts });
                    } else if (msg.message.state[j].start) {
                        if (typeof msg.message.state[j].start === 'string') {
                            msg.message.state[j].start = new Date(msg.message.state[j].start).getTime();
                        }
                        if (typeof msg.message.state[j].end === 'string') {
                            msg.message.state[j].end = new Date(msg.message.state[j].end).getTime();
                        }
                        await this._delete(id, {
                            start: msg.message.state[j].start,
                            end: msg.message.state[j].end || Date.now(),
                        });
                    }
                } else if (msg.message.state[j] && typeof msg.message.state[j] === 'number') {
                    await this._delete(id, { ts: msg.message.state[j] });
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
                }
            }
        } else if (msg.message.ts && Array.isArray(msg.message.ts)) {
            this.log.debug(`deleteStateFromDB ${msg.message.ts.length} items`);
            id = this._aliasMap[msg.message.id] ? this._aliasMap[msg.message.id] : msg.message.id;
            for (let j = 0; j < msg.message.ts.length; j++) {
                if (msg.message.ts[j] && typeof msg.message.ts[j] === 'number') {
                    await this._delete(id, { ts: msg.message.ts[j] });
                } else {
                    this.log.warn(`Invalid state for ${JSON.stringify(msg.message.ts[j])}`);
                }
            }
        } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
            this.log.debug('deleteStateFromDB 1 item');
            id = this._aliasMap[msg.message.id] ? this._aliasMap[msg.message.id] : msg.message.id;
            try {
                await this._delete(id, { ts: msg.message.state.ts });
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            } catch (error) {
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: false,
                        error: extractError(error),
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            }
        } else if (msg.message.id && msg.message.ts && typeof msg.message.ts === 'number') {
            this.log.debug('deleteStateFromDB 1 item');
            id = this._aliasMap[msg.message.id] ? this._aliasMap[msg.message.id] : msg.message.id;
            try {
                await this._delete(id, { ts: msg.message.ts });
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            } catch (error) {
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: false,
                        error: extractError(error),
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            }
        } else {
            this.log.error('deleteStateFromDB called with invalid data');
            this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
            return;
        }

        this.sendTo(
            msg.from,
            msg.command,
            {
                success: true,
                connected: !!this._connected,
            },
            msg.callback,
        );
    }

    async deleteStateAll(msg: ioBroker.Message): Promise<void> {
        if (!msg.message) {
            this.log.error('deleteStateAll called with invalid data');
            this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
            return;
        }
        let id: string;
        if (Array.isArray(msg.message)) {
            this.log.debug(`deleteStateAll ${msg.message.length} items`);
            try {
                for (let i = 0; i < msg.message.length; i++) {
                    id = this._aliasMap[msg.message[i].id] ? this._aliasMap[msg.message[i].id] : msg.message[i].id;
                    await this._delete(id, {});
                }
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            } catch (error) {
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: false,
                        error: extractError(error),
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            }
        } else if (msg.message.id) {
            this.log.debug('deleteStateAll 1 item');
            id = this._aliasMap[msg.message.id] ? this._aliasMap[msg.message.id] : msg.message.id;
            try {
                await this._delete(id, {});
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: true,
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            } catch (error) {
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: false,
                        error: extractError(error),
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            }
        } else {
            this.log.error('deleteStateAll called with invalid data');
            this.sendTo(msg.from, msg.command, { error: `Invalid call: ${JSON.stringify(msg)}` }, msg.callback);
        }
    }

    async update(id: string, state: ioBroker.State): Promise<void> {
        if (!this._connected) {
            throw new Error('not connected');
        }

        if (this.config.dbversion === '1.x') {
            const query = `SELECT * FROM "${id}" WHERE time = '${new Date(state.ts).toISOString()}'`;

            try {
                const result = await this._client?.query<
                    {
                        value?: ioBroker.StateValue;
                        val: ioBroker.StateValue;
                        ack: boolean;
                        q: number;
                        ts: number;
                        from: string;
                        time?: number;
                    }[]
                >(query);
                if (this._client) {
                    this.setConnected(true);
                }

                if (result?.[0]?.[0]) {
                    const stored = result[0][0];
                    const storedState: ioBroker.State = {
                        val: stored.val === undefined ? stored.value : stored.val,
                        ack: stored.ack,
                        q: stored.q,
                        ts: stored.ts,
                        from: stored.from,
                    } as ioBroker.State;

                    if (state.val !== undefined) {
                        storedState.val = state.val;
                    }
                    if (state.ack !== undefined) {
                        storedState.ack = state.ack;
                    }
                    if (state.q !== undefined) {
                        storedState.q = state.q;
                    }
                    if (state.from) {
                        storedState.from = state.from;
                    }
                    storedState.ts = state.ts;

                    await this._delete(id, { ts: new Date(stored.time || stored.ts).getTime() });
                    await this.pushValueIntoDB(id, storedState, true);
                } else {
                    this.log.error(`Cannot find value to delete for ${id}: ${JSON.stringify(state)}`);
                    throw new Error('not found');
                }
            } catch (error) {
                this.log.warn(`Error on update("${query}): ${error} / ${JSON.stringify(error.message)}"`);
                throw error;
            }
        } else if (this.config.dbversion === '2.x') {
            let fluxQuery = `from(bucket: "${this.config.dbname}") `;
            //using identical start/stops values leads to an 'empty range' error, therefore we add a microsecond
            fluxQuery += ` |> range(start:time(v:${state.ts * 1000000}), stop:time(v:${state.ts * 1000000 + 1000}))`;
            fluxQuery += ` |> filter(fn: (r) => r["_measurement"] == "${id}")`;
            fluxQuery += ` ${!this.config.usetags ? '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' : ''}`;

            try {
                const result = await this._client?.query<{
                    value?: ioBroker.StateValue;
                    val: ioBroker.StateValue;
                    ack: boolean;
                    q: number;
                    ts: number;
                    from: string;
                    time?: number;
                }>(fluxQuery);
                if (this._client) {
                    this.setConnected(true);
                }
                if (result?.length) {
                    const stored = result[0];
                    const storedState: ioBroker.State = {
                        val: stored.val === undefined ? stored.value : stored.val,
                        ack: stored.ack,
                        q: stored.q,
                        ts: stored.ts || stored.time,
                        from: stored.from,
                    } as ioBroker.State;
                    if (state.val !== undefined) {
                        storedState.val = state.val;
                    }
                    if (state.ack !== undefined) {
                        storedState.ack = state.ack;
                    }
                    if (state.q !== undefined) {
                        storedState.q = state.q;
                    }
                    if (state.from) {
                        storedState.from = state.from;
                    }
                    storedState.ts = state.ts;

                    try {
                        await this._delete(id, { ts: new Date(stored.time || stored.ts).getTime() });
                        await this.pushValueIntoDB(id, storedState, true);
                    } catch {
                        this.log.error(`Cannot delete value for ${id}: ${JSON.stringify(state)}`);
                    }
                } else {
                    this.log.error(`Cannot find value to delete for ${id}: ${JSON.stringify(state)}`);
                    throw new Error('not found');
                }
            } catch (error) {
                this.log.warn(`Error on update("${fluxQuery}): ${error} / ${JSON.stringify(error.message)}"`);
                throw error;
            }
        } else {
            throw new Error('not implemented');
        }
    }

    async updateState(msg: ioBroker.Message): Promise<void> {
        if (!msg.message) {
            this.log.error('updateState called with invalid data');
            this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
            return;
        }
        let id: string;
        if (Array.isArray(msg.message)) {
            this.log.debug(`updateState ${msg.message.length} items`);
            try {
                for (let i = 0; i < msg.message.length; i++) {
                    id = this._aliasMap[msg.message[i].id] ? this._aliasMap[msg.message[i].id] : msg.message[i].id;

                    if (msg.message[i].state && typeof msg.message[i].state === 'object') {
                        await this.update(id, msg.message[i].state);
                    } else {
                        this.log.warn(`Invalid state for ${JSON.stringify(msg.message[i])}`);
                    }
                }
                this.sendTo(msg.from, msg.command, { success: true, connected: !!this._connected }, msg.callback);
            } catch (error) {
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: false,
                        error: extractError(error),
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            }
        } else if (msg.message.state && Array.isArray(msg.message.state)) {
            this.log.debug(`updateState ${msg.message.state.length} items`);
            id = this._aliasMap[msg.message.id] ? this._aliasMap[msg.message.id] : msg.message.id;
            try {
                for (let j = 0; j < msg.message.state.length; j++) {
                    if (msg.message.state[j] && typeof msg.message.state[j] === 'object') {
                        await this.update(id, msg.message.state[j]);
                    } else {
                        this.log.warn(`Invalid state for ${JSON.stringify(msg.message.state[j])}`);
                    }
                }
                this.sendTo(msg.from, msg.command, { success: true, connected: !!this._connected }, msg.callback);
            } catch (error) {
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: false,
                        error: extractError(error),
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            }
        } else if (msg.message.id && msg.message.state && typeof msg.message.state === 'object') {
            this.log.debug('updateState 1 item');
            id = this._aliasMap[msg.message.id] ? this._aliasMap[msg.message.id] : msg.message.id;
            try {
                await this.update(id, msg.message.state);
                this.sendTo(msg.from, msg.command, { success: true, connected: !!this._connected }, msg.callback);
            } catch (error) {
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        success: !error,
                        error: extractError(error),
                        connected: !!this._connected,
                    },
                    msg.callback,
                );
            }
        } else {
            this.log.error('updateState called with invalid data');
            this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
        }
    }

    async storeStatePushData(
        id: string,
        state: ioBroker.State | null | undefined,
        applyRules?: boolean,
    ): Promise<void> {
        if (!state || typeof state !== 'object') {
            throw new Error(`State ${JSON.stringify(state)} for ${id} is not valid`);
        }

        if (!this._influxDPs[id]) {
            if (applyRules) {
                throw new Error(`influxdb not enabled for ${id}, so can not apply the rules as requested`);
            }
            this._influxDPs[id] = { realId: id, config: '{}' } as SavedInfluxDbCustomConfig;
        }
        try {
            if (applyRules) {
                await this.pushHistory(id, state);
            } else {
                await this.pushHelper(id, state);
            }
        } catch (error) {
            throw new Error(`Error writing state for ${id}: ${extractError(error)}, Data: ${JSON.stringify(state)}`);
        }
    }

    async storeState(msg: ioBroker.Message): Promise<void> {
        if (!msg.message) {
            this.log.error('storeState called with invalid data');
            this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
            return;
        }

        const errors = [];
        let successCount = 0;
        if (Array.isArray(msg.message)) {
            this.log.debug(`storeState ${msg.message.length} items`);
            for (let i = 0; i < msg.message.length; i++) {
                const id = this._aliasMap[msg.message[i].id] ? this._aliasMap[msg.message[i].id] : msg.message[i].id;
                try {
                    await this.storeStatePushData(id, msg.message[i].state, msg.message[i].rules);
                    successCount++;
                } catch (error) {
                    errors.push(extractError(error));
                }
            }
        } else if (msg.message.id && Array.isArray(msg.message.state)) {
            this.log.debug(`storeState ${msg.message.state.length} items`);
            const id = this._aliasMap[msg.message.id] ? this._aliasMap[msg.message.id] : msg.message.id;
            for (let j = 0; j < msg.message.state.length; j++) {
                try {
                    await this.storeStatePushData(id, msg.message.state[j], msg.message.rules);
                    successCount++;
                } catch (error) {
                    errors.push(extractError(error));
                }
            }
        } else if (msg.message.id && msg.message.state) {
            this.log.debug('storeState 1 item');
            const id = this._aliasMap[msg.message.id] ? this._aliasMap[msg.message.id] : msg.message.id;
            try {
                await this.storeStatePushData(id, msg.message.state, msg.message.rules);
                successCount++;
            } catch (error) {
                errors.push(extractError(error));
            }
        } else {
            this.log.error('storeState called with invalid data');
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call: ${JSON.stringify(msg)}`,
                },
                msg.callback,
            );
        }
        if (errors.length) {
            this.log.warn(`storeState executed with ${errors.length} errors: ${errors.join(', ')}`);
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `${errors.length} errors happened while storing data`,
                    errors,
                    successCount,
                },
                msg.callback,
            );
        }

        this.log.debug(`storeState executed with ${successCount} states successfully`);
        this.sendTo(
            msg.from,
            msg.command,
            {
                success: true,
                successCount,
                connected: !!this._connected,
                seriesBufferCounter: this._seriesBufferCounter,
                seriesBufferFlushPlanned: this._seriesBufferFlushPlanned,
            },
            msg.callback,
        );
    }

    async finish(callback?: () => void): Promise<void> {
        void this.setState?.('info.connection', false, true);

        if (!this._subscribeAll) {
            // unsubscribe
            for (const _id in this._influxDPs) {
                this.unsubscribeForeignStates(this._influxDPs[_id].realId);
            }
        } else {
            this._subscribeAll = false;
            this.unsubscribeForeignStates('*');
        }

        if (this._reconnectTimeout) {
            clearTimeout(this._reconnectTimeout);
            this._reconnectTimeout = null;
        }
        if (this._pingInterval) {
            clearInterval(this._pingInterval);
            this._pingInterval = null;
        }
        if (this._finished) {
            callback?.();
            return;
        }
        this._finished = true;
        if (this._seriesBufferChecker) {
            clearInterval(this._seriesBufferChecker);
            this._seriesBufferChecker = null;
        }
        for (const id in this._influxDPs) {
            if (!Object.prototype.hasOwnProperty.call(this._influxDPs, id)) {
                continue;
            }

            if (this._influxDPs[id].relogTimeout) {
                clearTimeout(this._influxDPs[id].relogTimeout);
                this._influxDPs[id].relogTimeout = null;
            }
            if (this._influxDPs[id].timeout) {
                clearTimeout(this._influxDPs[id].timeout);
                this._influxDPs[id].timeout = null;
            }

            try {
                if (this._influxDPs[id].skipped && !this._influxDPs[id].disableSkippedValueLogging) {
                    await this.pushHelper(id, this._influxDPs[id].skipped);
                    this._influxDPs[id].skipped = null;
                }
            } catch (error) {
                this.log.warn(`Error by bush: ${error}`);
            }
        }

        this.writeFileBufferToDisk();

        if (callback) {
            callback();
        } else {
            this.terminate ? this.terminate() : process.exit();
        }
    }

    async getHistoryV1(msg: ioBroker.Message): Promise<void> {
        const startTime = Date.now();

        if (!msg.message?.options) {
            this.sendTo(
                msg.from,
                msg.command,
                {
                    error: 'Invalid call. No options for getHistory provided',
                },
                msg.callback,
            );
            return;
        }
        const logId = (msg.message.id ? msg.message.id : 'all') + Date.now() + Math.random();
        let id: string | undefined = msg.message.id === '*' ? undefined : msg.message.id;

        const options: GetHistoryOptions = {
            start: msg.message.options.start,
            end: msg.message.options.end || new Date().getTime() + 5000000,
            step: parseInt(msg.message.options.step, 10) || undefined,
            count: parseInt(msg.message.options.count, 10),
            aggregate: msg.message.options.aggregate || 'average', // One of: max, min, average, total
            limit:
                parseInt(msg.message.options.limit, 10) ||
                parseInt(msg.message.options.count, 10) ||
                parseInt(this.config.limit as string, 10) ||
                2000,
            addId: msg.message.options.addId || false,
            ignoreNull: true,
            sessionId: msg.message.options.sessionId,
            returnNewestEntries: msg.message.options.returnNewestEntries || false,
            percentile:
                msg.message.options.aggregate === 'percentile'
                    ? parseInt(msg.message.options.percentile, 10) || 50
                    : undefined,
            quantile:
                msg.message.options.aggregate === 'quantile'
                    ? parseFloat(msg.message.options.quantile) || 0.5
                    : undefined,
            integralUnit:
                msg.message.options.aggregate === 'integral'
                    ? parseInt(msg.message.options.integralUnit, 10) || 60
                    : undefined,
            integralInterpolation:
                msg.message.options.aggregate === 'integral'
                    ? msg.message.options.integralInterpolation || 'none'
                    : undefined,
            removeBorderValues: msg.message.options.removeBorderValues || false,
            round: 0,
        };

        this.log.debug(`${logId} getHistory message: ${JSON.stringify(msg.message)}`);

        if (!options.count || isNaN(options.count)) {
            if (options.aggregate === 'none' || options.aggregate === 'onchange') {
                options.count = options.limit;
            } else {
                options.count = 500;
            }
        }

        try {
            if (options.start && typeof options.start !== 'number') {
                options.start = new Date(options.start).getTime();
            }
        } catch {
            this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call. Start date ${JSON.stringify(options.start)} is not a valid date`,
                },
                msg.callback,
            );
            return;
        }

        try {
            if (options.end && typeof options.end !== 'number') {
                options.end = new Date(options.end).getTime();
            }
        } catch {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call. End date ${JSON.stringify(options.end)} is not a valid date`,
                },
                msg.callback,
            );
        }

        if (!options.start && options.count) {
            options.returnNewestEntries = true;
        }

        if (
            msg.message.options.round !== null &&
            msg.message.options.round !== undefined &&
            msg.message.options.round !== ''
        ) {
            msg.message.options.round = parseInt(msg.message.options.round, 10);
            if (!isFinite(msg.message.options.round) || msg.message.options.round < 0) {
                options.round = this.config.round as number;
            } else {
                options.round = Math.pow(10, parseInt(msg.message.options.round, 10));
            }
        } else {
            options.round = this.config.round as number;
        }

        if (id) {
            this._influxDPs[id] ||= {} as SavedInfluxDbCustomConfig;
            this._influxDPs[id].enableDebugLogs = !!msg.message.options.enableDebugLogs;
            this._influxDPs[id].config ||= '{}';
        }
        const debugLog = (id && !!this._influxDPs[id]?.enableDebugLogs) || this.config.enableDebugLogs;

        if (id && this._aliasMap[id]) {
            id = this._aliasMap[id];
        }

        if ((options.aggregate === 'percentile' && options.percentile! < 0) || options.percentile! > 100) {
            this.log.error(`Invalid percentile value: ${options.percentile}, use 50 as default`);
            options.percentile = 50;
        }

        if ((options.aggregate === 'quantile' && options.quantile! < 0) || options.quantile! > 1) {
            this.log.error(`Invalid quantile value: ${options.quantile}, use 0.5 as default`);
            options.quantile = 0.5;
        }

        if (
            options.aggregate === 'integral' &&
            (typeof options.integralUnit !== 'number' || options.integralUnit <= 0)
        ) {
            this.log.error(`Invalid integralUnit value: ${options.integralUnit}, use 60s as default`);
            options.integralUnit = 60;
        }

        if (!options.start && !options.count) {
            options.start = options.end! - 86400000; // - 1 day
        }

        if (options.start! > options.end!) {
            const _end = options.end;
            options.end = options.start;
            options.start = _end;
        }

        if (debugLog) {
            this.log.debug(`${logId} getHistory (InfluxDB1) call: ${JSON.stringify(options)}`);
        }

        let resultsFromInfluxDB =
            !msg.message.useAdapter &&
            options.aggregate !== 'onchange' &&
            options.aggregate !== 'none' &&
            options.aggregate !== 'minmax';

        if (options.aggregate === 'integral' && options.integralInterpolation === 'linear') {
            resultsFromInfluxDB = false;
        }

        // query one timegroup-value more than requested originally at start and end
        // to make sure to have no 0 values because of the way InfluxDB does group by time

        if (resultsFromInfluxDB) {
            if (!options.step) {
                // calculate "step" based on difference between start and end using count
                options.step = Math.round((options.end! - options.start!) / options.count!);
            }
            if (options.start) {
                options.start -= options.step;
            }
            options.end! += options.step;
            options.limit! += 2;
        }

        options.preAggregated = true;
        let query = 'SELECT';
        if (options.step) {
            switch (options.aggregate) {
                case 'average':
                    query += ' mean(value) as val';
                    break;

                case 'max':
                    query += ' max(value) as val';
                    break;

                case 'min':
                    query += ' min(value) as val';
                    break;

                case 'percentile':
                    query += ` percentile(value, ${options.percentile}) as val`;
                    break;

                case 'quantile':
                    query += ` percentile(value, ${options.quantile! * 100}) as val`;
                    break;

                case 'integral':
                    if (options.integralInterpolation === 'linear') {
                        query += ' value';
                        options.preAggregated = false;
                    } else {
                        query += ` integral(value, ${options.integralUnit}s) as val`;
                    }
                    break;

                case 'total':
                    query += ' sum(value) as val';
                    break;

                case 'count':
                    query += ' count(value) as val';
                    break;

                case 'none':
                case 'onchange':
                case 'minmax':
                    query += ' value';
                    options.preAggregated = false;
                    break;

                default:
                    query += ' mean(value) as val';
                    break;
            }
        } else {
            query += ' *';
            options.preAggregated = false;
        }

        query += ` from "${id}"`;

        query += ` WHERE `;
        if (options.start) {
            query += ` time > '${new Date(options.start).toISOString()}' AND `;
        }
        query += ` time < '${new Date(options.end!).toISOString()}'`;

        if (resultsFromInfluxDB) {
            query += ` GROUP BY time(${options.step}ms) fill(previous)`;
        }

        if (
            (!options.start && options.count) ||
            (options.aggregate === 'none' && options.count && options.returnNewestEntries)
        ) {
            query += ` ORDER BY time DESC`;
        } else {
            query += ' ORDER BY time ASC';
        }

        if (resultsFromInfluxDB) {
            query += ` LIMIT ${options.limit}`;
        } else if (options.aggregate !== 'minmax' && options.aggregate !== 'integral') {
            query += ` LIMIT ${options.count}`;
        }

        // select one datapoint more than wanted
        if (!options.removeBorderValues) {
            let addQuery = '';
            if (options.start) {
                addQuery = `SELECT value from "${id}" WHERE time <= '${new Date(options.start).toISOString()}' ORDER BY time DESC LIMIT 1;`;
                query = addQuery + query;
            }
            addQuery = `;SELECT value from "${id}" WHERE time >= '${new Date(options.end!).toISOString()}' LIMIT 1`;
            query += addQuery;
        }

        if (debugLog) {
            this.log.debug(`${logId} History-Queries to execute: ${query}`);
        }

        try {
            const storedCount = await this.storeBufferedSeries(id);
            setTimeout(
                async () => {
                    if (!this._client) {
                        Aggregate.sendResponse(
                            this as unknown as ioBroker.Adapter,
                            msg,
                            id,
                            options,
                            'Database no longer connected',
                            startTime,
                        );
                        return;
                    }
                    try {
                        const rows = await this._client.query<
                            {
                                val: number;
                                value?: number;
                                time: number;
                                ts?: number;
                                q: number;
                                from: string;
                                ack: boolean;
                            }[]
                        >(query);
                        this.setConnected(true);
                        if (debugLog) {
                            this.log.debug(`${logId} Response rows: ${JSON.stringify(rows)}`);
                        }

                        const result: IobDataEntry[] = [];

                        if (rows?.length) {
                            for (let qr = 0; qr < rows.length; qr++) {
                                if (Array.isArray(rows[qr])) {
                                    for (let rr = 0; rr < rows[qr].length; rr++) {
                                        const storedItem = rows[qr][rr];
                                        const item: IobDataEntry = {
                                            ts: 0,
                                            val: null,
                                        };
                                        if (storedItem.val !== undefined) {
                                            item.val = storedItem.val;
                                        } else if (storedItem.value !== undefined) {
                                            item.val = storedItem.value!;
                                        }
                                        if (storedItem.time) {
                                            item.ts = new Date(storedItem.time).getTime();
                                        } else if (storedItem.ts) {
                                            item.ts = new Date(storedItem.ts).getTime();
                                        }
                                        if (storedItem.from) {
                                            item.from = storedItem.from;
                                        }
                                        if (storedItem.ack !== undefined) {
                                            item.ack = storedItem.ack;
                                        }
                                        if (storedItem.q !== undefined) {
                                            item.q = storedItem.q;
                                        }
                                        result.push(item);
                                    }
                                } else {
                                    // rows[qr] is already a value
                                    const storedItem = rows[qr] as unknown as {
                                        val: number;
                                        value?: number;
                                        time: number;
                                        ts?: number;
                                        q: number;
                                        from: string;
                                        ack: boolean;
                                    };
                                    const item: IobDataEntry = {
                                        ts: 0,
                                        val: null,
                                    };
                                    if (storedItem.val !== undefined) {
                                        item.val = storedItem.val;
                                    } else if (storedItem.value !== undefined) {
                                        item.val = storedItem.value!;
                                    }
                                    if (storedItem.time) {
                                        item.ts = new Date(storedItem.time).getTime();
                                    } else if (storedItem.ts) {
                                        item.ts = new Date(storedItem.ts).getTime();
                                    }
                                    if (storedItem.from) {
                                        item.from = storedItem.from;
                                    }
                                    if (storedItem.ack !== undefined) {
                                        item.ack = storedItem.ack;
                                    }
                                    if (storedItem.q !== undefined) {
                                        item.q = storedItem.q;
                                    }
                                    result.push(item);
                                }
                            }
                            result.sort(sortByTs);
                        }

                        try {
                            Aggregate.sendResponse(
                                this as unknown as ioBroker.Adapter,
                                msg,
                                id,
                                options,
                                result,
                                startTime,
                            );
                        } catch (e) {
                            Aggregate.sendResponse(
                                this as unknown as ioBroker.Adapter,
                                msg,
                                id,
                                options,
                                e.toString(),
                                startTime,
                            );
                        }
                    } catch (error) {
                        if (this._client.getHostsAvailable() === 0) {
                            this.setConnected(false);
                        }
                        this.log.error(`getHistory: ${extractError(error)}`);
                        Aggregate.sendResponse(
                            this as unknown as ioBroker.Adapter,
                            msg,
                            id,
                            options,
                            extractError(error),
                            startTime,
                        );
                    }
                },
                storedCount ? 50 : 0,
            );
        } catch (error) {
            this.log.info(`Error storing buffered series for ${id} before GetHistory: ${extractError(error)}`);
        }
    }

    async getHistoryV2(msg: ioBroker.Message): Promise<void> {
        const startTime = Date.now();

        if (!msg.message?.options) {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: 'Invalid call. No options for getHistory provided',
                },
                msg.callback,
            );
        }

        const options: GetHistoryOptions = {
            start: msg.message.options.start,
            end: msg.message.options.end || new Date().getTime() + 5000000,
            step: parseInt(msg.message.options.step, 10) || undefined,
            count: parseInt(msg.message.options.count, 10) || 500,
            aggregate: msg.message.options.aggregate || 'average', // One of: max, min, average, total
            limit:
                parseInt(msg.message.options.limit, 10) ||
                parseInt(msg.message.options.count, 10) ||
                parseInt(this.config.limit as string, 10) ||
                2000,
            addId: msg.message.options.addId || false,
            ignoreNull: true,
            sessionId: msg.message.options.sessionId,
            returnNewestEntries: msg.message.options.returnNewestEntries || false,
            percentile:
                msg.message.options.aggregate === 'percentile'
                    ? parseInt(msg.message.options.percentile, 10) || 50
                    : undefined,
            quantile:
                msg.message.options.aggregate === 'quantile'
                    ? parseFloat(msg.message.options.quantile) || 0.5
                    : undefined,
            integralUnit:
                msg.message.options.aggregate === 'integral'
                    ? parseInt(msg.message.options.integralUnit, 10) || 60
                    : undefined,
            integralInterpolation:
                msg.message.options.aggregate === 'integral'
                    ? msg.message.options.integralInterpolation || 'none'
                    : undefined,
            removeBorderValues: msg.message.options.removeBorderValues || false,
        };
        const logId = (msg.message.id ? msg.message.id : 'all') + Date.now() + Math.random();
        let id: string | undefined = msg.message.id === '*' ? undefined : msg.message.id;

        this.log.debug(`${logId} getHistory message: ${JSON.stringify(msg.message)}`);

        if (!options.count || isNaN(options.count)) {
            if (options.aggregate === 'none' || options.aggregate === 'onchange') {
                options.count = options.limit;
            } else {
                options.count = 500;
            }
        }

        try {
            if (options.start && typeof options.start !== 'number') {
                options.start = new Date(options.start).getTime();
            }
        } catch {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call. Start date ${JSON.stringify(options.start)} is not a valid date`,
                },
                msg.callback,
            );
        }

        try {
            if (options.end && typeof options.end !== 'number') {
                options.end = new Date(options.end).getTime();
            }
        } catch {
            return this.sendTo(
                msg.from,
                msg.command,
                {
                    error: `Invalid call. End date ${JSON.stringify(options.end)} is not a valid date`,
                },
                msg.callback,
            );
        }

        if (!options.start && options.count) {
            options.returnNewestEntries = true;
        }

        if (
            msg.message.options.round !== null &&
            msg.message.options.round !== undefined &&
            msg.message.options.round !== ''
        ) {
            msg.message.options.round = parseInt(msg.message.options.round, 10);
            if (!isFinite(msg.message.options.round) || msg.message.options.round < 0) {
                options.round = this.config.round as number;
            } else {
                options.round = Math.pow(10, parseInt(msg.message.options.round, 10));
            }
        } else {
            options.round = this.config.round as number;
        }

        if (id) {
            this._influxDPs[id] ||= {} as SavedInfluxDbCustomConfig;
            this._influxDPs[id].enableDebugLogs = !!msg.message.options.enableDebugLogs;
            this._influxDPs[id].config ||= '{}';
        }
        const debugLog = (id && !!this._influxDPs[id]?.enableDebugLogs) || this.config.enableDebugLogs;

        if (id && this._aliasMap[id]) {
            id = this._aliasMap[id];
        }

        if ((options.aggregate === 'percentile' && options.percentile! < 0) || options.percentile! > 100) {
            this.log.error(`Invalid percentile value: ${options.percentile}, use 50 as default`);
            options.percentile = 50;
        }

        if ((options.aggregate === 'quantile' && options.quantile! < 0) || options.quantile! > 1) {
            this.log.error(`Invalid quantile value: ${options.quantile}, use 0.5 as default`);
            options.quantile = 0.5;
        }

        if (
            options.aggregate === 'integral' &&
            (typeof options.integralUnit !== 'number' || options.integralUnit <= 0)
        ) {
            this.log.error(`Invalid integralUnit value: ${options.integralUnit}, use 60s as default`);
            options.integralUnit = 60;
        }
        if (!options.start && !options.count) {
            options.start = options.end! - 86400000; // - 1 day
        }

        if (options.start! > options.end!) {
            const _end = options.end;
            options.end = options.start;
            options.start = _end;
        }

        if (debugLog) {
            this.log.debug(`${logId} getHistory (InfluxDB2) options final: ${JSON.stringify(options)}`);
        }

        const resultsFromInfluxDB =
            options.aggregate !== 'onchange' && options.aggregate !== 'none' && options.aggregate !== 'minmax';

        // query one timegroup-value more than requested originally at start and end
        // to make sure to have no 0 values because of the way InfluxDB does group by time
        if (resultsFromInfluxDB) {
            if (!options.step) {
                // calculate "step" based on difference between start and end using count
                options.step = Math.round((options.end! - options.start!) / options.count!);
            }
            if (options.start) {
                options.start -= options.step;
            }
            options.end! += options.step;
            options.limit! += 2;
        }

        const valueColumn = this.config.usetags ? '_value' : 'value';

        // Workaround to detect if measurement is of type bool (to skip non-sensual aggregation options)
        // There seems to be no officially supported way to detect this, so we check it by forcing a type-conflict;
        const booleanTypeCheckQuery = `
        from(bucket: "${this.config.dbname}")
|> range(${options.start ? `start: ${new Date(options.start).toISOString()}, ` : `start: ${new Date(options.end! - ((this.config.retention as number) || 31536000) * 1000).toISOString()}, `}stop: ${new Date(options.end!).toISOString()})
|> filter(fn: (r) => r["_field"] == "value" and r["_measurement"] == "${id}" and contains(value: r._value, set: [true, false]))
${this.config.usetags ? ' |> duplicate(column: "_value", as: "value")' : ' |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'}
|> group()`;

        try {
            const storedCount = await this.storeBufferedSeries(id);
            setTimeout(
                async () => {
                    let supportsAggregates;
                    // Detect of measurement does support aggregates (is number)
                    try {
                        const result = await this._client?.query<{ error?: string }>(booleanTypeCheckQuery);
                        // If shutdown in between
                        if (id && !this._influxDPs[id]) {
                            return;
                        }
                        if (id && this._influxDPs[id]?.storageType && this._influxDPs[id].storageType !== 'Number') {
                            supportsAggregates = false;
                        } else {
                            if (debugLog) {
                                this.log.debug(`${logId} Bool check result: ${JSON.stringify(result)}`);
                            }
                            supportsAggregates = !!result?.find(r => r.error?.includes('type conflict: bool'));
                        }
                    } catch (error) {
                        if (extractError(error).includes('type conflict: bool')) {
                            if (debugLog) {
                                this.log.debug(`${logId} Bool check error: ${extractError(error)}`);
                            }
                            supportsAggregates = true;
                        } else {
                            // real error
                            this.sendTo(
                                msg.from,
                                msg.command,
                                {
                                    result: [],
                                    error: extractError(error),
                                    sessionId: options.sessionId,
                                },
                                msg.callback,
                            );
                            return;
                        }
                    }

                    if (supportsAggregates && id) {
                        const config = this._influxDPs[id];
                        if (config?.state && typeof config.state.val !== 'number') {
                            supportsAggregates = false;
                        } else if (config?.skipped && typeof config.skipped.val !== 'number') {
                            supportsAggregates = false;
                        }
                    }
                    if (!supportsAggregates && debugLog) {
                        this.log.debug(
                            `${logId} Measurement ${id} seems to be no number - skipping aggregation options`,
                        );
                    }

                    const fluxQueries: string[] = [];
                    let fluxQuery = `from(bucket: "${this.config.dbname}") `;

                    fluxQuery += ` |> range(${options.start ? `start: ${new Date(options.start).toISOString()}, ` : `start: ${new Date(options.end! - ((this.config.retention as number) || 31536000) * 1000).toISOString()}, `}stop: ${new Date(options.end!).toISOString()})`;
                    fluxQuery += ` |> filter(fn: (r) => r["_measurement"] == "${id}"${resultsFromInfluxDB && supportsAggregates ? ` and r["_field"] == "value"` : ''})`; // we cannot aggregate ack or from

                    if (!this.config.usetags) {
                        fluxQuery += ' |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")';
                    }

                    if (resultsFromInfluxDB && supportsAggregates) {
                        if (options.step !== undefined && options.step !== null && options.step > 0) {
                            fluxQuery += ` |> window(every: ${options.step}ms)`;
                        }
                        fluxQuery += `|> fill(column: "${valueColumn}", usePrevious: true)`;
                    } else if (options.aggregate !== 'minmax') {
                        fluxQuery += ` |> group()`;
                    }

                    if (
                        (!options.start && options.count) ||
                        (options.aggregate === 'none' && options.count && options.returnNewestEntries)
                    ) {
                        fluxQuery += ` |> sort(columns:["_time"], desc: true)`;
                    } else {
                        fluxQuery += ` |> sort(columns:["_time"], desc: false)`;
                    }

                    if (!(resultsFromInfluxDB && supportsAggregates) && options.aggregate !== 'minmax') {
                        fluxQuery += ` |> limit(n: ${options.count})`;
                    }

                    if (resultsFromInfluxDB && options.step && supportsAggregates) {
                        options.preAggregated = true;
                        switch (options.aggregate) {
                            case 'average':
                                fluxQuery += ` |> mean(column: "${valueColumn}")`;
                                break;

                            case 'max':
                                fluxQuery += ` |> max(column: "${valueColumn}")`;
                                break;

                            case 'min':
                                fluxQuery += ` |> min(column: "${valueColumn}")`;
                                break;

                            case 'percentile':
                                fluxQuery += ` |> quantile(column: "${valueColumn}", q: ${options.percentile! / 100})`;
                                break;

                            case 'quantile':
                                fluxQuery += ` |> quantile(column: "${valueColumn}", q: ${options.quantile})`;
                                break;

                            case 'integral':
                                fluxQuery += ` |> integral(column: "${valueColumn}", unit: ${options.integralUnit}s, interpolate: ${options.integralInterpolation === 'linear' ? '"linear"' : '""'})`;
                                break;

                            case 'total':
                                fluxQuery += ` |> sum(column: "${valueColumn}")`;
                                break;

                            case 'count':
                                fluxQuery += ` |> count(column: "${valueColumn}")`;
                                break;

                            default:
                                fluxQuery += ` |> mean(column: "${valueColumn}")`;
                                options.preAggregated = false;
                                break;
                        }
                    }

                    if (this.config.usetags) {
                        fluxQuery += ' |> duplicate(column: "_value", as: "value")';
                    }

                    fluxQueries.push(fluxQuery);

                    // select one datapoint more than wanted
                    if (!options.removeBorderValues) {
                        let addFluxQuery = '';
                        if (options.start) {
                            // get one entry "before" the defined timeframe for displaying purposes
                            addFluxQuery = `from(bucket: "${this.config.dbname}") 
|> range(start: ${new Date(options.start - ((this.config.retention as number) || 31536000) * 1000).toISOString()}, stop: ${new Date(options.start - 1).toISOString()}) 
|> filter(fn: (r) => r["_measurement"] == "${id}"${resultsFromInfluxDB && supportsAggregates ? ` and r["_field"] == "value"` : ''}) 
|> last()
${!this.config.usetags ? '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' : ''}`;

                            fluxQueries.unshift(addFluxQuery);
                        }
                        // get one entry "after" the defined timeframe for displaying purposes
                        addFluxQuery = `from(bucket: "${this.config.dbname}") 
|> range(start: ${new Date(options.end! + 1).toISOString()}) 
|> filter(fn: (r) => r["_measurement"] == "${id}"${resultsFromInfluxDB && supportsAggregates ? ` and r["_field"] == "value"` : ''}) 
|> first()
${!this.config.usetags ? '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' : ''}`;
                        fluxQueries.push(addFluxQuery);
                    }

                    if (debugLog) {
                        this.log.debug(`${logId} History-queries to execute: ${fluxQueries.join('\n---\n')}`);
                    }

                    try {
                        // if specific id requested
                        const rows = await this._client?.queries<{
                            val?: number;
                            value?: number;
                            _start?: number;
                            _stop?: number;
                            _measurement?: string;
                            time?: number;
                            ts?: number;
                            from?: string;
                            ack?: boolean;
                            q?: number;
                        }>(fluxQueries);
                        if (!rows) {
                            if (!this._client?.getHostsAvailable()) {
                                this.setConnected(false);
                            }
                        } else {
                            this.setConnected(true);
                        }
                        if (debugLog) {
                            this.log.debug(`${logId} Parsing retrieved rows:${JSON.stringify(rows)}`);
                        }

                        const result: IobDataEntry[] = [];

                        if (rows?.length) {
                            for (let qr = 0; qr < rows.length; qr++) {
                                for (let rr = 0; rr < rows[qr].length; rr++) {
                                    const item: IobDataEntry = {
                                        ts: 0,
                                        val: null,
                                    };
                                    const storedItem = rows[qr][rr];
                                    if (storedItem.val !== undefined) {
                                        item.val = storedItem.val;
                                    } else if (storedItem.value !== undefined) {
                                        item.val = storedItem.value!;
                                    }
                                    if (storedItem.time) {
                                        item.ts = new Date(storedItem.time).getTime();
                                    } else if (storedItem.ts) {
                                        item.ts = new Date(storedItem.ts).getTime();
                                    } else if (storedItem._start && storedItem._stop) {
                                        const startTime = new Date(storedItem._start).getTime();
                                        const stopTime = new Date(storedItem._stop).getTime();
                                        item.ts = Math.round(startTime + (stopTime - startTime) / 2);
                                    }
                                    if (storedItem.from) {
                                        item.from = storedItem.from;
                                    }
                                    if (storedItem.ack !== undefined) {
                                        item.ack = storedItem.ack;
                                    }
                                    if (storedItem.q !== undefined) {
                                        item.q = storedItem.q;
                                    }

                                    if (typeof item.val === 'number' && options.round) {
                                        item.val = Math.round(item.val * options.round) / options.round;
                                    }
                                    result.push(item);
                                }
                            }
                            result.sort(sortByTs);
                        }

                        try {
                            Aggregate.sendResponse(
                                this as unknown as ioBroker.Adapter,
                                msg,
                                id,
                                options,
                                result,
                                startTime,
                            );
                        } catch (e) {
                            Aggregate.sendResponse(
                                this as unknown as ioBroker.Adapter,
                                msg,
                                id,
                                options,
                                e.toString(),
                                startTime,
                            );
                        }
                    } catch (error) {
                        if (!this._client?.getHostsAvailable()) {
                            this.setConnected(false);
                        }
                        this.log.error(`getHistory: ${extractError(error)}`);
                    }
                },
                storedCount ? 50 : 0,
            );
        } catch (error) {
            this.log.info(`Error storing buffered series for ${id} before GetHistory: ${extractError(error)}`);
        }
    }

    async query(msg: ioBroker.Message): Promise<void> {
        if (this._client) {
            const query = msg.message.query || msg.message;

            if (!query || typeof query !== 'string') {
                this.log.error(`query missing: ${query}`);
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        result: [],
                        error: 'Query missing',
                    },
                    msg.callback,
                );
                return;
            }

            this.log.debug(`query: ${query}`);

            try {
                let rows = await this._client?.query<
                    {
                        time?: number;
                        ts?: number;
                        _start: number;
                        _stop: number;
                        value: number;
                        val: number;
                    }[]
                >(query);
                this.setConnected(true);
                if (Array.isArray(rows) && rows.length && !Array.isArray(rows[0])) {
                    // The new influx lib returns only one array, sp reformat it because of backwards compatibility
                    rows = [rows as any];
                }

                this.log.debug(`result: ${JSON.stringify(rows)}`);

                // as the answer from DB could have any type as we request not only time series, just try to format time and value
                for (let r = 0, l = rows.length; r < l; r++) {
                    for (let rr = 0, ll = rows[r].length; rr < ll; rr++) {
                        const item = rows[r][rr];
                        if (item.time) {
                            item.ts = new Date(item.time).getTime();
                            delete item.time;
                        } else if (item._start && item._stop) {
                            const startTime = new Date(item._start).getTime();
                            const stopTime = new Date(item._stop).getTime();
                            item.ts = startTime + (stopTime - startTime) / 2;
                        } else if (item.value !== undefined) {
                            item.val = item.value;
                        }
                    }
                }

                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        result: rows,
                        ts: Date.now(),
                    },
                    msg.callback,
                );
            } catch (error) {
                if (!this._client.getHostsAvailable()) {
                    this.setConnected(false);
                }
                this.log.error(`query: ${extractError(error)}`);
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        result: [],
                        error: extractError(error),
                    },
                    msg.callback,
                );
            }
        } else {
            this.sendTo(
                msg.from,
                msg.command,
                {
                    result: [],
                    error: 'No connection',
                },
                msg.callback,
            );
        }
    }

    async multiQuery(msg: ioBroker.Message): Promise<void> {
        if (this._client) {
            const queriesString = msg.message.query || msg.message;

            let queries;
            try {
                //parse queries to array
                queries = queriesString.split(';');

                for (let q = 0, len = queries.length; q < len; q++) {
                    if (!queries[q] || typeof queries[q] !== 'string') {
                        throw new Error(`Array element #${q + 1}: Query messing`);
                    }
                }
            } catch (error) {
                this.log.warn(`Error in received multiQuery: ${extractError(error)}`);
                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        result: [],
                        error: extractError(error),
                    },
                    msg.callback,
                );
                return;
            }
            this.log.debug(`queries: ${queries}`);

            try {
                let rows = await this._client?.query<
                    {
                        time?: number;
                        ts?: number;
                        value: number;
                        val: number;
                        q: number;
                        from: string;
                        ack: boolean;
                    }[]
                >(queries);
                this.setConnected(true);
                if (Array.isArray(rows) && rows.length && !Array.isArray(rows[0])) {
                    // The new influx lib returns only one array, sp reformat it because of backwards compatibility
                    rows = [rows as any];
                }

                this.log.debug(`result: ${JSON.stringify(rows)}`);

                // as the answer from DB could have any type as we request not only time series, just try to format time and value
                for (let r = 0, l = rows.length; r < l; r++) {
                    for (let rr = 0, ll = rows[r].length; rr < ll; rr++) {
                        const item = rows[r][rr];
                        if (item.time) {
                            item.ts = new Date(item.time).getTime();
                            delete item.time;
                        } else if (item.value !== undefined) {
                            item.val = item.value;
                        }
                    }
                }

                this.sendTo(
                    msg.from,
                    msg.command,
                    {
                        result: rows,
                        ts: Date.now(),
                    },
                    msg.callback,
                );
            } catch (error) {
                if (error) {
                    if (!this._client?.getHostsAvailable()) {
                        this.setConnected(false);
                    }
                    this.log.error(`queries: ${extractError(error)}`);
                    return this.sendTo(
                        msg.from,
                        msg.command,
                        {
                            result: [],
                            error: extractError(error),
                        },
                        msg.callback,
                    );
                }
            }
        } else {
            this.sendTo(
                msg.from,
                msg.command,
                {
                    result: [],
                    error: 'No connection',
                },
                msg.callback,
            );
        }
    }

    enableHistory(msg: ioBroker.Message): void {
        if (!msg.message?.id) {
            this.log.error('enableHistory called with invalid data');
            this.sendTo(msg.from, msg.command, { error: 'Invalid call' }, msg.callback);
            return;
        }
        const obj = {
            common: {
                custom: {
                    [this.namespace]: {},
                },
            },
        } as ioBroker.StateObject;
        if (obj.common.custom) {
            if (msg.message.options) {
                obj.common.custom[this.namespace] = msg.message.options;
            }
            obj.common.custom[this.namespace].enabled = true;
        }
        this.extendForeignObject(msg.message.id, obj, error => {
            if (error) {
                this.log.error(`enableHistory: ${error}`);
                this.sendTo(msg.from, msg.command, { error }, msg.callback);
            } else {
                this.log.info(JSON.stringify(obj));
                this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
            }
        });
    }

    disableHistory(msg: ioBroker.Message): void {
        if (!msg.message || !msg.message.id) {
            this.log.error('disableHistory called with invalid data');
            this.sendTo(msg.from, msg.command, { error: 'Invalid call' }, msg.callback);
            return;
        }
        const obj = {
            common: {
                custom: { [this.namespace]: { enabled: false } },
            },
        } as ioBroker.StateObject;
        this.extendForeignObject(msg.message.id, obj, error => {
            if (error) {
                this.log.error(`disableHistory: ${error}`);
                this.sendTo(msg.from, msg.command, { error }, msg.callback);
            } else {
                this.log.info(JSON.stringify(obj));
                this.sendTo(msg.from, msg.command, { success: true }, msg.callback);
            }
        });
    }

    getEnabledDPs(msg: ioBroker.Message): void {
        const data: { [id: string]: InfluxDbCustomConfigTyped } = {};
        for (const id in this._influxDPs) {
            if (Object.prototype.hasOwnProperty.call(this._influxDPs, id) && this._influxDPs[id]?.enabled) {
                data[this._influxDPs[id].realId] = JSON.parse(this._influxDPs[id].config);
            }
        }

        this.sendTo(msg.from, msg.command, data, msg.callback);
    }
}

// If started as allInOne mode => return function to create instance
if (require.main !== module) {
    // Export the constructor in compact mode
    module.exports = (options: Partial<AdapterOptions> | undefined) => new InfluxDBAdapter(options);
} else {
    // otherwise start the instance directly
    (() => new InfluxDBAdapter())();
}
