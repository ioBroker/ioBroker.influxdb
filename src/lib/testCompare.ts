import type { ContainerConfig } from '@iobroker/plugin-docker';

const desiredConfig: ContainerConfig = {
    iobEnabled: true,
    iobMonitoringEnabled: true,
    iobAutoImageUpdate: false,
    image: 'influxdb:2',
    name: 'iobroker_influxdb.0',
    ports: [
        {
            hostPort: 8086,
            containerPort: 8086,
            hostIP: '127.0.0.1',
        },
    ],
    mounts: [
        {
            source: '/opt/iobroker/iobroker-data/influxdb.0/data',
            target: '/var/lib/influxdb2',
            type: 'bind',
        },
        {
            source: '/opt/iobroker/iobroker-data/influxdb.0/config',
            target: '/etc/influxdb2',
            type: 'bind',
        },
    ],
    environment: {
        DOCKER_INFLUXDB_INIT_USERNAME: 'iobroker',
        DOCKER_INFLUXDB_INIT_PASSWORD: 'iobroker',
        DOCKER_INFLUXDB_INIT_BUCKET: 'iobroker',
        DOCKER_INFLUXDB_INIT_ORG: 'iobroker',
        DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: 'aW9icm9rZXI4NjY0NTYzODU0NjU2NTY1MjY1Ng==',
    },
};

const existingConfig: ContainerConfig = {
    image: 'influxdb:2',
    name: 'iobroker_influxdb.0',
    command: ['influxd'],
    entrypoint: ['/entrypoint.sh'],
    user: '',
    workdir: '',
    hostname: 'a57b18ae7ca1',
    domainname: '',
    macAddress: '',
    environment: {
        DOCKER_INFLUXDB_INIT_USERNAME: 'iobroker',
        DOCKER_INFLUXDB_INIT_PASSWORD: 'iobroker',
        DOCKER_INFLUXDB_INIT_BUCKET: 'iobroker',
        DOCKER_INFLUXDB_INIT_ORG: 'iobroker',
        DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: 'aW9icm9rZXI4NjY0NTYzODU0NjU2NTY1MjY1Ng==',
        DOCKER_INFLUXDB_INIT_MODE: 'setup',
        PATH: '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
        GOSU_VER: '1.16',
        INFLUXDB_VERSION: '2.7.12',
        INFLUX_CLI_VERSION: '2.7.5',
        INFLUX_CONFIGS_PATH: '/etc/influxdb2/influx-configs',
        INFLUXD_INIT_PORT: '9999',
        INFLUXD_INIT_PING_ATTEMPTS: '600',
        DOCKER_INFLUXDB_INIT_CLI_CONFIG_NAME: 'default',
    },
    labels: {},
    tty: false,
    stdinOpen: false,
    attachStdin: false,
    attachStdout: false,
    attachStderr: false,
    openStdin: false,
    publishAllPorts: false,
    ports: [
        {
            containerPort: '8086',
            protocol: 'tcp',
            hostPort: '8086',
            hostIP: '127.0.0.1',
        },
    ],
    mounts: [
        {
            type: 'bind',
            source: '/opt/iobroker/iobroker-data/influxdb.0/data',
            target: '/var/lib/influxdb2',
            readOnly: true,
        },
        {
            type: 'bind',
            source: '/opt/iobroker/iobroker-data/influxdb.0/config',
            target: '/etc/influxdb2',
            readOnly: true,
        },
    ],
    volumes: ['/etc/influxdb2', '/var/lib/influxdb2'],
    dns: {
        servers: [],
        search: [],
        options: [],
    },
    networkMode: 'bridge',
    networks: [
        {
            name: 'bridge',
            ipv4Address: '',
            ipv6Address: '',
        },
    ],
    restart: {
        policy: 'no',
        maxRetries: 0,
    },
    resources: {
        cpuShares: 0,
        cpuQuota: 0,
        cpuPeriod: 0,
        cpusetCpus: '',
        memory: 0,
        memorySwap: 0,
        memoryReservation: 0,
        shmSize: 67108864,
        readOnlyRootFilesystem: false,
    },
    logging: {
        driver: 'json-file',
        options: {},
    },
    security: {
        privileged: false,
        usernsMode: '',
        ipc: 'private',
        pid: '',
        apparmor: '',
    },
    stop: {},
    readOnly: false,
};

const dockerDefaults: Record<string, any> = {
    tty: false,
    stdinOpen: false,
    attachStdin: false,
    attachStdout: false,
    attachStderr: false,
    openStdin: false,
    publishAllPorts: false,
    readOnly: false,
    user: '',
    workdir: '',
    domainname: '',
    macAddress: '',
    networkMode: 'bridge',
};

function isDefault(value: any, def: any): boolean {
    return JSON.stringify(value) === JSON.stringify(def);
}

function deepCompare(object1: any, object2: any): boolean {
    if (typeof object1 === 'number') {
        object1 = object1.toString();
    }
    if (typeof object2 === 'number') {
        object2 = object2.toString();
    }
    if (typeof object1 !== typeof object2) {
        return false;
    }
    if (typeof object1 !== 'object' || object1 === null || object2 === null) {
        return object1 === object2;
    }
    if (Array.isArray(object1)) {
        if (!Array.isArray(object2) || object1.length !== object2.length) {
            return false;
        }
        for (let i = 0; i < object1.length; i++) {
            if (!deepCompare(object1[i], object2[i])) {
                return false;
            }
        }
        return true;
    }
    const keys1 = Object.keys(object1);
    const keys2 = Object.keys(object2);
    if (keys1.length !== keys2.length) {
        return false;
    }
    for (const key of keys1) {
        if (!deepCompare(object1[key], object2[key])) {
            return false;
        }
    }
    return true;
}

function compareConfigs(desired: ContainerConfig, existing: ContainerConfig): string[] {
    const diffs: string[] = [];

    const keys: (keyof ContainerConfig)[] = Object.keys(desired) as Array<keyof ContainerConfig>;

    // We only compare keys that are in the desired config
    for (const key of keys) {
        // ignore iob* properties as they belong to ioBroker configuration
        if (key.startsWith('iob')) {
            continue;
        }
        // ignore hostname
        if (key === 'hostname') {
            continue;
        }
        if (typeof desired[key] === 'object' && desired[key] !== null) {
            if (Array.isArray(desired[key])) {
                if (!Array.isArray(existing[key]) || desired[key].length !== existing[key].length) {
                    diffs.push(key);
                } else {
                    for (let i = 0; i < desired[key]!.length; i++) {
                        if (!deepCompare(desired[key]![i], existing[key]![i])) {
                            diffs.push(`${key}[${i}]`);
                        }
                    }
                }
            } else {
                Object.keys(desired[key]!).forEach((subKey: string) => {
                    if (
                        !deepCompare(
                            ((desired as any)[key] as any)[subKey],
                            ((existing as any)[key] as any)[subKey],
                        )
                    ) {
                        diffs.push(`${key}.${subKey}`);
                    }
                });
            }
        } else if (desired[key] !== existing[key]) {
            diffs.push(key);
        }
    }

    return diffs;
}

// remove undefined entries recursively
function removeUndefined(obj: any): any {
    if (Array.isArray(obj)) {
        const arr = obj.map(v => (v && typeof v === 'object' ? removeUndefined(v) : v)).filter(v => v !== undefined);
        if (!arr.length) {
            return undefined;
        }
        return arr;
    }
    if (obj && typeof obj === 'object') {
        const _obj = Object.fromEntries(
            Object.entries(obj)
                .map(([k, v]) => [k, v && typeof v === 'object' ? removeUndefined(v) : v])
                .filter(
                    ([_, v]) =>
                        v !== undefined &&
                        v !== null &&
                        v !== '' &&
                        !(Array.isArray(v) && v.length === 0) &&
                        !(typeof v === 'object' && Object.keys(v).length === 0),
                ),
        );
        if (Object.keys(_obj).length === 0) {
            return undefined;
        }
        return _obj;
    }
    if (obj === '') {
        return undefined;
    }
    return obj;
}

function doIt(): void {
    const existingConfigClean = removeUndefined(existingConfig);
    Object.keys(existingConfigClean).forEach(name => {
        if (isDefault((existingConfigClean as any)[name], (dockerDefaults as any)[name])) {
            delete (existingConfigClean as any)[name];
        }
        if (name === 'mounts') {
            (existingConfigClean as any)[name] = (existingConfigClean as any)[name].map((mount: any) => {
                const m = { ...mount };
                delete m.readOnly;
                return m;
            });
        }
        if (name === 'ports') {
            (existingConfigClean as any)[name] = (existingConfigClean as any)[name].map((port: any) => {
                const p = { ...port };
                if (p.protocol === 'tcp') {
                    delete p.protocol;
                }
                return p;
            });
        }
    });

    console.log(compareConfigs(desiredConfig, existingConfigClean));
}
doIt();
