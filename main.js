'use strict';

const utils = require('@iobroker/adapter-core');
const axios = require('axios');
const http = require('http');
const mqtt = require('mqtt');

const MODE = {
    PV_EXPORT: 1,
    PV_BATTERY_LAST: 2,
    GRID_MANUAL: 3,
};

const MODE_LABEL = {
    [MODE.PV_EXPORT]: 'PV only',
    [MODE.PV_BATTERY_LAST]: 'PV only (go-e = priority)',
    [MODE.GRID_MANUAL]: 'grid mode',
};

const PHASE_MODE_LABEL = {
    0: 'Automatik',
    1: '1-phasig',
    2: '3-phasig',
};
const DEFAULT_CURRENT_STEPS_A = [6, 10, 12, 14, 16];

class GoEGeminiAdapter extends utils.Adapter {
    constructor(options = {}) {
        super({
            ...options,
            name: 'go-e-gemini-adapter',
        });

        this.on('ready', this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('unload', this.onUnload.bind(this));

        this.http = null;
        this.pollTimer = null;
        this.mqttClient = null;

        this.runtime = {
            charger: {
                connected: false,
                firmware: '',
                serial: '',
                httpReadFailStreak: 0,
                chargingAllowed: false,
                carState: 0,
                chargerPowerW: 0,
                chargerCurrentA: 0,
                setCurrentA: 0,
                setCurrentVolatileA: 0,
                enabledPhases: 0,
                actualPhaseMode: 0,
                allowedCurrentsA: [...DEFAULT_CURRENT_STEPS_A],
            },
            command: {
                lastSentAtMs: 0,
                lastAllow: null,
                lastCurrentA: null,
                lastCurrentKey: null,
                lastPhaseMode: null,
                lastHttpUnavailableLogAtMs: 0,
            },
            phaseControl: {
                stableMode: 1,
                candidateMode: null,
                candidateSinceMs: 0,
            },
            allowControl: {
                stableAllow: false,
                startCandidateSinceMs: 0,
                stopCandidateSinceMs: 0,
            },
            controlMeta: {
                currentStepSignature: '',
            },
            evaluationQueue: Promise.resolve(),
            httpQueue: Promise.resolve(),
            httpTransientCooldownUntilMs: 0,
            session: {
                active: false,
                energyWh: 0,
                lastSampleTsMs: 0,
                lastPowerW: 0,
            },
        };
    }

    async onReady() {
        try {
            this.normalizeConfig();
            this.http = axios.create({
                baseURL: `http://${this.config.chargerHost}`,
                timeout: this.config.httpTimeoutMs,
                httpAgent: new http.Agent({ keepAlive: false, maxSockets: 1 }),
            });

            await this.ensureObjects();
            await this.initializeControlStates();
            this.subscribeStates('control.*');

            this.registerForeignSubscriptions();

            if (this.config.readTransport === 'mqtt' || this.config.readTransport === 'hybrid' || this.config.writeTransport === 'mqtt') {
                this.connectMqtt();
            }

            if (this.config.readTransport === 'http' || this.config.readTransport === 'hybrid') {
                await this.readStatusHttp();
            }

            try {
                await this.enqueueEvaluation('startup');
            } catch (err) {
                const msg = `Startup evaluation failed: ${err.message || err}`;
                await this.setStateAck('diagnostics.lastError', msg);
                if (this.isTransientNetworkError(err)) {
                    this.log.warn(msg);
                } else {
                    this.log.error(msg);
                }
            }

            this.pollTimer = this.setInterval(async () => {
                try {
                    if (this.config.readTransport === 'http' || this.config.readTransport === 'hybrid') {
                        await this.readStatusHttp();
                    }
                    await this.enqueueEvaluation('poll');
                } catch (err) {
                    if (this.isTransientNetworkError(err)) {
                        this.log.warn(`Polling/evaluation transient network failure: ${err.message || err}`);
                    } else {
                        this.log.error(`Polling/evaluation error: ${err.message || err}`);
                    }
                }
            }, this.config.pollIntervalSec * 1000);
        } catch (err) {
            this.log.error(`Adapter startup failed: ${err.message || err}`);
            await this.setStateAck('info.connection', false);
        }
    }

    normalizeConfig() {
        const cfg = this.config;

        cfg.chargerHost = (cfg.chargerHost || '192.168.44.110').trim();
        cfg.pollIntervalSec = this.clampInt(cfg.pollIntervalSec, 10, 2, 300);
        cfg.httpTimeoutMs = this.clampInt(cfg.httpTimeoutMs, 3000, 500, 20000);

        cfg.readTransport = ['http', 'mqtt', 'hybrid'].includes(cfg.readTransport) ? cfg.readTransport : 'http';
        cfg.writeTransport = ['http', 'mqtt'].includes(cfg.writeTransport) ? cfg.writeTransport : 'http';

        cfg.enableApiV2 = !!cfg.enableApiV2;

        cfg.mqttBrokerUrl = (cfg.mqttBrokerUrl || '').trim();
        cfg.mqttTopicPrefix = (cfg.mqttTopicPrefix || 'go-eCharger').trim();
        cfg.mqttSerial = (cfg.mqttSerial || '').trim();

        cfg.reservePowerW = this.clampNumber(cfg.reservePowerW, 300, 0, 100000);
        cfg.phaseSwitchUpThresholdW = this.clampNumber(cfg.phaseSwitchUpThresholdW, 4200, 500, 100000);
        cfg.phaseSwitchHysteresisW = this.clampNumber(cfg.phaseSwitchHysteresisW, 700, 0, 50000);
        cfg.phaseSwitchMinHoldSec = this.clampInt(cfg.phaseSwitchMinHoldSec, 120, 0, 7200);
        cfg.startDelaySec = this.clampInt(cfg.startDelaySec, 5, 0, 7200);
        cfg.stopDelaySec = this.clampInt(cfg.stopDelaySec, 20, 0, 7200);
        if (cfg.startDelaySec === 20 && cfg.stopDelaySec === 120) {
            cfg.startDelaySec = 5;
            cfg.stopDelaySec = 20;
            this.log.info('Migrated legacy start/stop delay defaults to 5s/20s');
        }
        cfg.maxInputAgeSec = this.clampInt(cfg.maxInputAgeSec, 30, 1, 3600);
        cfg.maxGridImportW = this.clampInt(cfg.maxGridImportW, -1, -1, 100000);
        cfg.defaultSimulationMode = !!cfg.defaultSimulationMode;

        cfg.minCurrentA = this.normalizeCurrentToStep(this.clampInt(cfg.minCurrentA, 6, 6, 32), 6, 32, 'up') || DEFAULT_CURRENT_STEPS_A[0];
        cfg.maxCurrentA = this.normalizeCurrentToStep(this.clampInt(cfg.maxCurrentA, 16, cfg.minCurrentA, 32), 6, 32, 'down') || DEFAULT_CURRENT_STEPS_A[DEFAULT_CURRENT_STEPS_A.length - 1];
        if (cfg.minCurrentA > cfg.maxCurrentA) {
            cfg.maxCurrentA = cfg.minCurrentA;
        }
        cfg.commandMinIntervalMs = this.clampInt(cfg.commandMinIntervalMs, 1500, 200, 30000);

        cfg.defaultMode = this.clampInt(cfg.defaultMode, MODE.PV_EXPORT, MODE.PV_EXPORT, MODE.GRID_MANUAL);
        cfg.defaultGridCurrentA = this.normalizeCurrentToStep(this.clampInt(cfg.defaultGridCurrentA, 10, 6, 32), cfg.minCurrentA, cfg.maxCurrentA, 'nearest') || cfg.minCurrentA;
        cfg.defaultGridPhaseMode = this.clampInt(cfg.defaultGridPhaseMode, 0, 0, 2);

        cfg.defaultTargetSocPercent = this.clampInt(cfg.defaultTargetSocPercent, 80, 1, 100);
        cfg.defaultTargetSocEnabled = !!cfg.defaultTargetSocEnabled;
    }

    async ensureObjects() {
        const currentSteps = this.getAllowedCurrentSteps();
        const currentStepStates = this.buildCurrentStepStateMap(currentSteps);
        const currentMin = currentSteps[0] || 6;
        const currentMax = currentSteps[currentSteps.length - 1] || 16;

        const defs = [
            { id: 'control', type: 'channel', common: { name: 'Control' } },
            { id: 'control.allowCharging', type: 'state', common: { name: 'Global allowCharging', role: 'switch.enable', type: 'boolean', read: true, write: true, def: true } },
            { id: 'control.emergencyStop', type: 'state', common: { name: 'Emergency stop (immediate, bypasses start/stop delays)', role: 'switch.enable', type: 'boolean', read: true, write: true, def: false } },
            { id: 'control.simulationMode', type: 'state', common: { name: 'Simulation mode (dry run, no write to charger)', role: 'switch.enable', type: 'boolean', read: true, write: true, def: this.config.defaultSimulationMode } },
            {
                id: 'control.mode',
                type: 'state',
                common: {
                    name: 'Charging mode',
                    role: 'value',
                    type: 'number',
                    read: true,
                    write: true,
                    def: this.config.defaultMode,
                    states: {
                        1: MODE_LABEL[1],
                        2: MODE_LABEL[2],
                        3: MODE_LABEL[3],
                    },
                },
            },
            { id: 'control.gridManual', type: 'channel', common: { name: 'Grid manual mode settings' } },
            {
                id: 'control.gridManual.currentA',
                type: 'state',
                common: {
                    name: 'Grid mode: target current [A]',
                    role: 'level.current',
                    type: 'number',
                    unit: 'A',
                    read: true,
                    write: true,
                    def: this.config.defaultGridCurrentA,
                    min: currentMin,
                    max: currentMax,
                    states: currentStepStates,
                },
            },
            {
                id: 'control.gridManual.phaseMode',
                type: 'state',
                common: {
                    name: 'Grid mode: phase mode (0 auto / 1 single / 2 three)',
                    role: 'value',
                    type: 'number',
                    read: true,
                    write: true,
                    def: this.config.defaultGridPhaseMode,
                    states: {
                        0: PHASE_MODE_LABEL[0],
                        1: PHASE_MODE_LABEL[1],
                        2: PHASE_MODE_LABEL[2],
                    },
                },
            },
            {
                id: 'control.minCurrentA',
                type: 'state',
                common: {
                    name: 'Minimum charging current [A]',
                    role: 'value',
                    type: 'number',
                    read: true,
                    write: true,
                    def: this.config.minCurrentA,
                    min: currentMin,
                    max: currentMax,
                    states: currentStepStates,
                },
            },
            {
                id: 'control.maxCurrentA',
                type: 'state',
                common: {
                    name: 'Maximum charging current [A]',
                    role: 'value',
                    type: 'number',
                    read: true,
                    write: true,
                    def: this.config.maxCurrentA,
                    min: currentMin,
                    max: currentMax,
                    states: currentStepStates,
                },
            },
            {
                id: 'control.targetSocEnabled',
                type: 'state',
                common: {
                    name: 'Enable target SoC limit',
                    role: 'switch.enable',
                    type: 'boolean',
                    read: true,
                    write: true,
                    def: this.config.defaultTargetSocEnabled,
                },
            },
            {
                id: 'control.targetSocPercent',
                type: 'state',
                common: {
                    name: 'Target SoC [%]',
                    role: 'level',
                    type: 'number',
                    read: true,
                    write: true,
                    unit: '%',
                    def: this.config.defaultTargetSocPercent,
                    min: 1,
                    max: 100,
                },
            },

            { id: 'status', type: 'channel', common: { name: 'Status' } },
            { id: 'status.connection', type: 'state', common: { name: 'Charger connection', role: 'indicator.connected', type: 'boolean', read: true, write: false, def: false } },
            { id: 'status.activeMode', type: 'state', common: { name: 'Active mode name', role: 'text', type: 'string', read: true, write: false, def: '' } },
            { id: 'status.decision', type: 'state', common: { name: 'Last decision', role: 'text', type: 'string', read: true, write: false, def: '' } },
            { id: 'status.effectiveAllowCharging', type: 'state', common: { name: 'Effective allowCharging', role: 'switch.enable', type: 'boolean', read: true, write: false, def: false } },
            { id: 'status.actualPhaseMode', type: 'state', common: { name: 'Actual phase mode (psm)', role: 'value', type: 'number', read: true, write: false, def: 0, states: { 0: PHASE_MODE_LABEL[0], 1: PHASE_MODE_LABEL[1], 2: PHASE_MODE_LABEL[2] } } },
            { id: 'status.targetPhaseMode', type: 'state', common: { name: 'Target phase mode', role: 'value', type: 'number', read: true, write: false, def: 0, states: { 0: PHASE_MODE_LABEL[0], 1: PHASE_MODE_LABEL[1], 2: PHASE_MODE_LABEL[2] } } },
            { id: 'status.enabledPhases', type: 'state', common: { name: 'Enabled phases', role: 'value', type: 'number', read: true, write: false, unit: 'phase', def: 0 } },
            { id: 'status.chargerPowerW', type: 'state', common: { name: 'Current charger power', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },
            { id: 'status.chargerCurrentA', type: 'state', common: { name: 'Current charger current', role: 'value.current', type: 'number', read: true, write: false, unit: 'A', def: 0 } },
            { id: 'status.sessionActive', type: 'state', common: { name: 'Charging session active', role: 'indicator', type: 'boolean', read: true, write: false, def: false } },
            { id: 'status.sessionEnergyWh', type: 'state', common: { name: 'Energy charged in current/last session', role: 'value.power.consumption', type: 'number', read: true, write: false, unit: 'Wh', def: 0 } },
            { id: 'status.sessionEnergyKWh', type: 'state', common: { name: 'Energy charged in current/last session', role: 'value.power.consumption', type: 'number', read: true, write: false, unit: 'kWh', def: 0 } },
            { id: 'status.setCurrentA', type: 'state', common: { name: 'Configured charger current (amp)', role: 'value.current', type: 'number', read: true, write: false, unit: 'A', def: 0 } },
            { id: 'status.setCurrentVolatileA', type: 'state', common: { name: 'Configured volatile charger current (amx)', role: 'value.current', type: 'number', read: true, write: false, unit: 'A', def: 0 } },
            { id: 'status.allowedCurrentStepsA', type: 'state', common: { name: 'Allowed charging current steps [A]', role: 'text', type: 'string', read: true, write: false, def: DEFAULT_CURRENT_STEPS_A.join(',') } },
            { id: 'status.carState', type: 'state', common: { name: 'Car state', role: 'value', type: 'number', read: true, write: false, def: 0 } },
            { id: 'status.carSocPercent', type: 'state', common: { name: 'Car SoC from foreign datapoint', role: 'level', type: 'number', read: true, write: false, unit: '%', def: 0 } },
            { id: 'status.lastCommand', type: 'state', common: { name: 'Last sent command', role: 'text', type: 'string', read: true, write: false, def: '' } },
            { id: 'status.lastCommandAt', type: 'state', common: { name: 'Last command timestamp', role: 'date', type: 'string', read: true, write: false, def: '' } },
            { id: 'status.transportRead', type: 'state', common: { name: 'Configured read transport', role: 'text', type: 'string', read: true, write: false, def: this.config.readTransport } },
            { id: 'status.transportWrite', type: 'state', common: { name: 'Configured write transport', role: 'text', type: 'string', read: true, write: false, def: this.config.writeTransport } },
            { id: 'status.simulationModeActive', type: 'state', common: { name: 'Simulation mode active', role: 'indicator', type: 'boolean', read: true, write: false, def: this.config.defaultSimulationMode } },

            { id: 'inputs', type: 'channel', common: { name: 'Input values (external datapoints)' } },
            { id: 'inputs.gridExportW', type: 'state', common: { name: 'Grid export', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },
            { id: 'inputs.gridImportW', type: 'state', common: { name: 'Grid import', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },
            { id: 'inputs.pvPowerW', type: 'state', common: { name: 'PV power', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },
            { id: 'inputs.houseConsumptionW', type: 'state', common: { name: 'House consumption', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },
            { id: 'inputs.homeBatteryChargeW', type: 'state', common: { name: 'Home battery charging power', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },
            { id: 'inputs.homeBatteryDischargeW', type: 'state', common: { name: 'Home battery discharging power', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },

            { id: 'calculation', type: 'channel', common: { name: 'Calculation details' } },
            { id: 'calculation.formula', type: 'state', common: { name: 'Active formula', role: 'text', type: 'string', read: true, write: false, def: '' } },
            { id: 'calculation.availablePowerW', type: 'state', common: { name: 'Available power after buffers', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },
            { id: 'calculation.nonEvHouseConsumptionW', type: 'state', common: { name: 'House consumption without EV (mode 2)', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },
            { id: 'calculation.targetCurrentRawA', type: 'state', common: { name: 'Target current before clamp', role: 'value.current', type: 'number', read: true, write: false, unit: 'A', def: 0 } },
            { id: 'calculation.targetCurrentFinalA', type: 'state', common: { name: 'Target current after clamp/delay', role: 'value.current', type: 'number', read: true, write: false, unit: 'A', def: 0 } },
            { id: 'calculation.phaseSwitchUpThresholdW', type: 'state', common: { name: 'Configured phase switch up threshold', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: this.config.phaseSwitchUpThresholdW } },
            { id: 'calculation.phaseSwitchDownThresholdW', type: 'state', common: { name: 'Calculated phase switch down threshold', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: this.config.phaseSwitchUpThresholdW - this.config.phaseSwitchHysteresisW } },
            { id: 'calculation.maxGridImportW', type: 'state', common: { name: 'Configured max grid import limit', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: this.config.maxGridImportW } },
            { id: 'calculation.gridImportLimitExceededW', type: 'state', common: { name: 'Grid import over configured limit', role: 'value.power', type: 'number', read: true, write: false, unit: 'W', def: 0 } },
            { id: 'calculation.socLimitReached', type: 'state', common: { name: 'SoC limit reached', role: 'indicator', type: 'boolean', read: true, write: false, def: false } },

            { id: 'diagnostics', type: 'channel', common: { name: 'Diagnostics' } },
            { id: 'diagnostics.readSource', type: 'state', common: { name: 'Last read source', role: 'text', type: 'string', read: true, write: false, def: '' } },
            { id: 'diagnostics.lastError', type: 'state', common: { name: 'Last error', role: 'text', type: 'string', read: true, write: false, def: '' } },
            { id: 'diagnostics.httpReadFailStreak', type: 'state', common: { name: 'Consecutive failed HTTP status reads', role: 'value', type: 'number', read: true, write: false, def: 0 } },
            { id: 'diagnostics.inputsStale', type: 'state', common: { name: 'One or more required input datapoints are stale', role: 'indicator', type: 'boolean', read: true, write: false, def: false } },
            { id: 'diagnostics.staleInputList', type: 'state', common: { name: 'List of stale/missing required input datapoints', role: 'text', type: 'string', read: true, write: false, def: '' } },
            { id: 'diagnostics.oldestInputAgeSec', type: 'state', common: { name: 'Oldest required input age', role: 'value.interval', type: 'number', read: true, write: false, unit: 's', def: 0 } },
            { id: 'diagnostics.maxInputAgeSec', type: 'state', common: { name: 'Configured max input age', role: 'value.interval', type: 'number', read: true, write: false, unit: 's', def: this.config.maxInputAgeSec } },
        ];

        for (const def of defs) {
            await this.setObjectNotExistsAsync(def.id, {
                type: def.type,
                common: def.common,
                native: {},
            });
        }
    }

    async initializeControlStates() {
        await this.updateCurrentControlStateMeta(true);

        await this.ensureStateDefault('control.allowCharging', true);
        await this.ensureStateDefault('control.emergencyStop', false);
        await this.ensureStateDefault('control.simulationMode', this.config.defaultSimulationMode);
        await this.ensureStateDefault('control.mode', this.config.defaultMode);
        await this.ensureStateDefault('control.gridManual.currentA', this.config.defaultGridCurrentA);
        await this.ensureStateDefault('control.gridManual.phaseMode', this.config.defaultGridPhaseMode);
        await this.ensureStateDefault('control.minCurrentA', this.config.minCurrentA);
        await this.ensureStateDefault('control.maxCurrentA', this.config.maxCurrentA);
        await this.ensureStateDefault('control.targetSocEnabled', this.config.defaultTargetSocEnabled);
        await this.ensureStateDefault('control.targetSocPercent', this.config.defaultTargetSocPercent);

        await this.setStateAck('status.transportRead', this.config.readTransport);
        await this.setStateAck('status.transportWrite', this.config.writeTransport);
        await this.setStateAck('status.simulationModeActive', this.config.defaultSimulationMode);
        await this.setStateAck('status.sessionActive', false);
        await this.setStateAck('status.sessionEnergyWh', 0);
        await this.setStateAck('status.sessionEnergyKWh', 0);
        await this.setStateAck('status.allowedCurrentStepsA', this.getAllowedCurrentSteps().join(','));
        await this.setStateAck('calculation.phaseSwitchUpThresholdW', this.config.phaseSwitchUpThresholdW);
        await this.setStateAck('calculation.phaseSwitchDownThresholdW', this.getPhaseSwitchDownThreshold());
        await this.setStateAck('calculation.maxGridImportW', this.config.maxGridImportW);
        await this.setStateAck('diagnostics.maxInputAgeSec', this.config.maxInputAgeSec);
        await this.setStateAck('diagnostics.httpReadFailStreak', 0);
    }

    registerForeignSubscriptions() {
        const ids = [
            this.config.gridExportObjectId,
            this.config.gridImportObjectId,
            this.config.pvPowerObjectId,
            this.config.houseConsumptionObjectId,
            this.config.homeBatteryChargeObjectId,
            this.config.homeBatteryDischargeObjectId,
            this.config.carSocObjectId,
        ].filter(Boolean);

        for (const id of ids) {
            this.subscribeForeignStates(id);
        }
    }

    connectMqtt() {
        if (!this.config.mqttBrokerUrl) {
            this.log.warn('MQTT transport configured but mqttBrokerUrl is empty.');
            return;
        }
        const options = {
            username: this.config.mqttUsername || undefined,
            password: this.config.mqttPassword || undefined,
            reconnectPeriod: 3000,
        };

        this.mqttClient = mqtt.connect(this.config.mqttBrokerUrl, options);

        this.mqttClient.on('connect', () => {
            this.log.info(`MQTT connected to ${this.config.mqttBrokerUrl}`);
            if (this.config.readTransport === 'mqtt' || this.config.readTransport === 'hybrid') {
                const statusTopic = this.getMqttStatusTopic();
                this.mqttClient.subscribe(statusTopic, err => {
                    if (err) {
                        this.log.warn(`MQTT subscribe error on ${statusTopic}: ${err.message || err}`);
                    } else {
                        this.log.info(`Subscribed to ${statusTopic}`);
                    }
                });
            }
        });

        this.mqttClient.on('message', async (topic, payloadBuffer) => {
            try {
                const payload = payloadBuffer.toString('utf8');
                if (topic === this.getMqttStatusTopic()) {
                    const status = JSON.parse(payload);
                    this.processV1Status(status, 'mqtt');
                    await this.setStateAck('diagnostics.readSource', 'mqtt');
                    await this.enqueueEvaluation('mqtt-status');
                }
            } catch (err) {
                this.log.warn(`MQTT message parse error: ${err.message || err}`);
                await this.setStateAck('diagnostics.lastError', `MQTT parse error: ${err.message || err}`);
            }
        });

        this.mqttClient.on('error', async err => {
            this.log.warn(`MQTT error: ${err.message || err}`);
            await this.setStateAck('diagnostics.lastError', `MQTT error: ${err.message || err}`);
        });

        this.mqttClient.on('close', () => {
            this.log.warn('MQTT connection closed');
        });
    }

    getMqttStatusTopic() {
        return `${this.config.mqttTopicPrefix}/${this.config.mqttSerial}/status`;
    }

    getMqttCommandTopic() {
        return `${this.config.mqttTopicPrefix}/${this.config.mqttSerial}/cmd/req`;
    }

    async onStateChange(id, state) {
        if (!state || state.ack) {
            return;
        }

        try {
            const shortId = id.replace(`${this.namespace}.`, '');
            const normalized = this.normalizeControlStateValue(shortId, state.val);
            if (normalized === undefined) {
                this.log.debug(`Unhandled writable state: ${shortId}`);
                return;
            }

            await this.setStateAck(shortId, normalized);
            await this.enqueueEvaluation(`state-change:${shortId}`);
        } catch (err) {
            if (this.isTransientNetworkError(err)) {
                this.log.warn(`State change handling transient network failure: ${err.message || err}`);
            } else {
                this.log.error(`State change handling failed: ${err.message || err}`);
            }
        }
    }

    enqueueEvaluation(trigger) {
        const run = this.runtime.evaluationQueue
            .catch(() => undefined)
            .then(() => this.evaluateAndApply(trigger));
        this.runtime.evaluationQueue = run;
        return run;
    }

    enqueueHttpRequest(task, context = 'HTTP request') {
        const run = this.runtime.httpQueue
            .catch(() => undefined)
            .then(async () => {
                const waitMs = this.runtime.httpTransientCooldownUntilMs - Date.now();
                if (waitMs > 0) {
                    this.log.debug(`${context} waiting for transient cooldown: ${waitMs}ms`);
                    await this.delay(waitMs);
                }
                return task();
            });
        this.runtime.httpQueue = run;
        return run;
    }

    isTransientNetworkError(err) {
        const code = String(err?.code || err?.cause?.code || '').toUpperCase();
        const msg = String(err?.message || '').toUpperCase();
        if (['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ECONNABORTED'].includes(code)) {
            return true;
        }
        return msg.includes('SOCKET HANG UP') || msg.includes('READ ECONNRESET');
    }

    async httpGetWithRetry(path, options = {}, context = 'HTTP request') {
        return this.enqueueHttpRequest(async () => {
            const attempts = 3;
            let lastError = null;
            for (let attempt = 1; attempt <= attempts; attempt++) {
                try {
                    return await this.http.get(path, options);
                } catch (err) {
                    lastError = err;
                    if (!this.isTransientNetworkError(err) || attempt === attempts) {
                        if (this.isTransientNetworkError(err)) {
                            this.runtime.httpTransientCooldownUntilMs = Date.now() + 2500;
                        }
                        throw err;
                    }
                    const waitMs = 200 * attempt;
                    this.log.debug(`${context} transient failure (attempt ${attempt}/${attempts}): ${err.message || err}; retry in ${waitMs}ms`);
                    await this.delay(waitMs);
                }
            }
            throw lastError || new Error(`${context} failed`);
        }, context);
    }

    normalizeControlStateValue(shortId, value) {
        switch (shortId) {
            case 'control.mode':
                return this.clampInt(value, this.config.defaultMode, MODE.PV_EXPORT, MODE.GRID_MANUAL);
            case 'control.gridManual.phaseMode':
                return this.clampInt(value, this.config.defaultGridPhaseMode, 0, 2);
            case 'control.gridManual.currentA':
                return this.normalizeCurrentToStep(
                    this.clampInt(value, this.config.defaultGridCurrentA, 6, 32),
                    6,
                    32,
                    'nearest',
                ) ?? this.config.defaultGridCurrentA;
            case 'control.minCurrentA':
                return this.normalizeCurrentToStep(
                    this.clampInt(value, this.config.minCurrentA, 6, 32),
                    6,
                    32,
                    'up',
                ) ?? DEFAULT_CURRENT_STEPS_A[0];
            case 'control.maxCurrentA':
                return this.normalizeCurrentToStep(
                    this.clampInt(value, this.config.maxCurrentA, 6, 32),
                    6,
                    32,
                    'down',
                ) ?? DEFAULT_CURRENT_STEPS_A[DEFAULT_CURRENT_STEPS_A.length - 1];
            case 'control.targetSocPercent':
                return this.clampInt(value, this.config.defaultTargetSocPercent, 1, 100);
            case 'control.allowCharging':
            case 'control.emergencyStop':
            case 'control.simulationMode':
            case 'control.targetSocEnabled':
                return this.normalizeBoolean(value, false);
            default:
                return undefined;
        }
    }

    async readStatusHttp() {
        try {
            const r1 = await this.httpGetWithRetry('/status', { transformResponse: data => data }, 'HTTP v1 status read');
            const statusV1 = JSON.parse(r1.data);
            this.processV1Status(statusV1, 'http-v1');

            if (this.config.enableApiV2) {
                try {
                    const filter = 'psm,pnp,pha,alw,amp,car';
                    const r2 = await this.httpGetWithRetry(`/api/status?filter=${filter}`, { transformResponse: data => data }, 'HTTP v2 status read');
                    const statusV2 = JSON.parse(r2.data);
                    this.processV2Status(statusV2, 'http-v2');
                } catch (errV2) {
                    this.log.debug(`HTTP v2 status read failed: ${errV2.message || errV2}`);
                }
            }

            this.runtime.charger.connected = true;
            this.runtime.charger.httpReadFailStreak = 0;
            await this.setStateAck('info.connection', true);
            await this.setStateAck('status.connection', true);
            await this.setStateAck('diagnostics.httpReadFailStreak', 0);
            await this.setStateAck('diagnostics.readSource', 'http');
        } catch (err) {
            this.runtime.charger.connected = false;
            this.runtime.charger.httpReadFailStreak += 1;
            await this.setStateAck('info.connection', false);
            await this.setStateAck('status.connection', false);
            await this.setStateAck('diagnostics.httpReadFailStreak', this.runtime.charger.httpReadFailStreak);
            await this.setStateAck('diagnostics.lastError', `HTTP read failed: ${err.message || err}`);
            this.log.warn(`HTTP read failed: ${err.message || err}`);
        }
    }

    processV1Status(status, source) {
        if (!status || typeof status !== 'object') {
            return;
        }

        const charger = this.runtime.charger;

        charger.firmware = String(status.fwv || charger.firmware || '');
        charger.serial = String(status.sse || charger.serial || '');

        if (status.alw !== undefined) {
            charger.chargingAllowed = Number(status.alw) === 1 || status.alw === true;
        }
        if (status.car !== undefined) {
            charger.carState = Number(status.car) || 0;
        }
        if (status.amp !== undefined) {
            charger.setCurrentA = Number(status.amp) || 0;
        }
        if (status.amx !== undefined) {
            charger.setCurrentVolatileA = Number(status.amx) || 0;
        }
        const currentSteps = this.extractCurrentStepsFromStatus(status);
        if (currentSteps.length) {
            charger.allowedCurrentsA = currentSteps;
            void this.setStateAck('status.allowedCurrentStepsA', currentSteps.join(','));
            void this.updateCurrentControlStateMeta();
        }
        if (status.pha !== undefined) {
            const pha = Number(status.pha) || 0;
            charger.enabledPhases = ((32 & pha) >> 5) + ((16 & pha) >> 4) + ((8 & pha) >> 3);
        }

        const hasPowerSample = Array.isArray(status.nrg) && status.nrg.length >= 7;
        if (hasPowerSample) {
            const v1 = Number(status.nrg[0]) || 0;
            const v2 = Number(status.nrg[1]) || 0;
            const v3 = Number(status.nrg[2]) || 0;
            const a1 = (Number(status.nrg[4]) || 0) / 10;
            const a2 = (Number(status.nrg[5]) || 0) / 10;
            const a3 = (Number(status.nrg[6]) || 0) / 10;
            charger.chargerCurrentA = this.round1(a1 + a2 + a3);
            charger.chargerPowerW = Math.max(0, Math.round(v1 * a1 + v2 * a2 + v3 * a3));
        }
        if (hasPowerSample) {
            this.updateSessionEnergyTracking(charger.chargerPowerW);
        }

        void this.setStateAck('status.chargerPowerW', charger.chargerPowerW);
        void this.setStateAck('status.chargerCurrentA', charger.chargerCurrentA);
        void this.setStateAck('status.setCurrentA', charger.setCurrentA);
        void this.setStateAck('status.setCurrentVolatileA', charger.setCurrentVolatileA);
        void this.setStateAck('status.carState', charger.carState);
        void this.setStateAck('status.enabledPhases', charger.enabledPhases);
        void this.setStateAck('status.effectiveAllowCharging', charger.chargingAllowed);
        void this.setStateAck('diagnostics.readSource', source);
    }

    updateSessionEnergyTracking(powerW) {
        const session = this.runtime.session;
        const nowMs = Date.now();
        const currentPowerW = Math.max(0, Math.round(Number(powerW) || 0));
        const startThresholdW = 250;
        const stopThresholdW = 80;
        const maxSampleGapSec = 120;

        if (session.active) {
            if (session.lastSampleTsMs > 0) {
                const dtSec = (nowMs - session.lastSampleTsMs) / 1000;
                if (dtSec > 0 && dtSec <= maxSampleGapSec) {
                    const avgPowerW = Math.max(0, (session.lastPowerW + currentPowerW) / 2);
                    session.energyWh += (avgPowerW * dtSec) / 3600;
                }
            }

            if (currentPowerW <= stopThresholdW) {
                session.active = false;
                session.lastSampleTsMs = 0;
            } else {
                session.lastSampleTsMs = nowMs;
            }
            session.lastPowerW = currentPowerW;
        } else if (currentPowerW >= startThresholdW) {
            session.active = true;
            session.energyWh = 0;
            session.lastSampleTsMs = nowMs;
            session.lastPowerW = currentPowerW;
        } else {
            session.lastSampleTsMs = 0;
            session.lastPowerW = currentPowerW;
        }

        const energyWhRounded = Math.max(0, Math.round(session.energyWh));
        void this.setStateAck('status.sessionActive', session.active);
        void this.setStateAck('status.sessionEnergyWh', energyWhRounded);
        void this.setStateAck('status.sessionEnergyKWh', this.round3(energyWhRounded / 1000));
    }

    processV2Status(status, source) {
        if (!status || typeof status !== 'object') {
            return;
        }

        const charger = this.runtime.charger;

        if (status.psm !== undefined) {
            charger.actualPhaseMode = Number(status.psm) || 0;
            void this.setStateAck('status.actualPhaseMode', charger.actualPhaseMode);
        }

        if (status.alw !== undefined) {
            charger.chargingAllowed = status.alw === true || Number(status.alw) === 1;
            void this.setStateAck('status.effectiveAllowCharging', charger.chargingAllowed);
        }

        if (status.amp !== undefined) {
            charger.setCurrentA = Number(status.amp) || charger.setCurrentA;
            void this.setStateAck('status.setCurrentA', charger.setCurrentA);
        }

        if (status.car !== undefined) {
            charger.carState = Number(status.car) || charger.carState;
            void this.setStateAck('status.carState', charger.carState);
        }

        void this.setStateAck('diagnostics.readSource', source);
    }

    async evaluateAndApply(trigger) {
        const inputs = await this.readInputValues();

        await this.setStateAck('inputs.gridExportW', inputs.gridExportW);
        await this.setStateAck('inputs.gridImportW', inputs.gridImportW);
        await this.setStateAck('inputs.pvPowerW', inputs.pvPowerW);
        await this.setStateAck('inputs.houseConsumptionW', inputs.houseConsumptionW);
        await this.setStateAck('inputs.homeBatteryChargeW', inputs.homeBatteryChargeW);
        await this.setStateAck('inputs.homeBatteryDischargeW', inputs.homeBatteryDischargeW);
        await this.setStateAck('status.carSocPercent', inputs.carSocPercent);

        const control = await this.readControlStates();
        await this.setStateAck('status.simulationModeActive', control.simulationMode);

        const minCurrentA = Math.min(control.minCurrentA, control.maxCurrentA);
        const maxCurrentA = Math.max(control.maxCurrentA, minCurrentA);

        const chargerPowerW = this.runtime.charger.chargerPowerW || 0;
        const freshness = this.evaluateInputFreshness(inputs, control.mode);

        await this.setStateAck('diagnostics.inputsStale', freshness.stale);
        await this.setStateAck('diagnostics.staleInputList', freshness.staleKeys.join(', '));
        await this.setStateAck('diagnostics.oldestInputAgeSec', freshness.oldestAgeSec);
        await this.setStateAck('diagnostics.maxInputAgeSec', this.config.maxInputAgeSec);
        await this.setStateAck('calculation.maxGridImportW', this.config.maxGridImportW);

        let activeFormula = '';
        let availablePowerW = 0;
        let gridImportLimitExceededW = 0;
        let nonEvHouseConsumptionW = 0;
        let targetPhaseMode = 0;
        let targetCurrentRawA = 0;
        let targetCurrentFinalA = 0;
        let decision = `trigger=${trigger}; `;

        let socLimitReached = false;
        if (control.targetSocEnabled && Number.isFinite(inputs.carSocPercent) && inputs.carSocPercent > 0 && inputs.carSocPercent >= control.targetSocPercent) {
            socLimitReached = true;
        }

        if (control.mode === MODE.GRID_MANUAL) {
            activeFormula = 'GRID: no power formula, use manual current and phase settings';
            targetPhaseMode = this.clampInt(control.gridPhaseMode, 0, 0, 2);
            targetCurrentRawA = control.gridCurrentA;
            targetCurrentFinalA = targetCurrentRawA;
            availablePowerW = Number.NaN;
            gridImportLimitExceededW = 0;
            decision += `gridManual current=${targetCurrentFinalA}A phaseMode=${targetPhaseMode}; `;
        } else if (control.mode === MODE.PV_EXPORT) {
            activeFormula = 'PV_EXPORT: available = gridExportW - gridImportW - reservePowerW';
            availablePowerW = inputs.gridExportW - inputs.gridImportW - this.config.reservePowerW;
            if (this.config.maxGridImportW >= 0) {
                gridImportLimitExceededW = Math.max(0, inputs.gridImportW - this.config.maxGridImportW);
                availablePowerW -= gridImportLimitExceededW;
            }
            targetPhaseMode = this.calculateDynamicPhaseMode(availablePowerW);
            const phases = targetPhaseMode === 2 ? 3 : 1;
            targetCurrentRawA = Math.floor(availablePowerW / (230 * phases));
            targetCurrentFinalA = this.normalizeCurrentToStep(targetCurrentRawA, minCurrentA, maxCurrentA, 'down') ?? 0;
            decision += `pvExport available=${Math.round(availablePowerW)}W targetRaw=${targetCurrentRawA}A phaseMode=${targetPhaseMode}; `;
        } else {
            activeFormula = 'PV_BATTERY_LAST: available = pvPowerW - (houseConsumptionW - chargerPowerW) + batteryChargeW - batteryDischargeW - reservePowerW';
            nonEvHouseConsumptionW = Math.max(0, inputs.houseConsumptionW - chargerPowerW);
            availablePowerW = inputs.pvPowerW - nonEvHouseConsumptionW + inputs.homeBatteryChargeW - inputs.homeBatteryDischargeW - this.config.reservePowerW;
            if (this.config.maxGridImportW >= 0) {
                gridImportLimitExceededW = Math.max(0, inputs.gridImportW - this.config.maxGridImportW);
                availablePowerW -= gridImportLimitExceededW;
            }
            targetPhaseMode = this.calculateDynamicPhaseMode(availablePowerW);
            const phases = targetPhaseMode === 2 ? 3 : 1;
            targetCurrentRawA = Math.floor(availablePowerW / (230 * phases));
            targetCurrentFinalA = this.normalizeCurrentToStep(targetCurrentRawA, minCurrentA, maxCurrentA, 'down') ?? 0;
            decision += `pvBatteryLast available=${Math.round(availablePowerW)}W targetRaw=${targetCurrentRawA}A phaseMode=${targetPhaseMode}; `;
        }

        let rawAllow = control.allowCharging && !socLimitReached;
        let currentKey = 'amx';

        if (control.mode === MODE.GRID_MANUAL) {
            if (targetCurrentFinalA < minCurrentA) {
                targetCurrentFinalA = minCurrentA;
            }
            rawAllow = rawAllow && true;
            currentKey = 'amp';
        } else {
            if (targetCurrentRawA < minCurrentA || availablePowerW <= 0) {
                rawAllow = false;
            }
            if (targetCurrentFinalA < minCurrentA) {
                rawAllow = false;
            }
            if (freshness.stale) {
                rawAllow = false;
            }
            currentKey = this.supportsAmx() ? 'amx' : 'amp';
        }

        if (control.emergencyStop) {
            this.runtime.allowControl.stableAllow = false;
            this.runtime.allowControl.startCandidateSinceMs = 0;
            this.runtime.allowControl.stopCandidateSinceMs = 0;
        }
        const effectiveAllow = control.emergencyStop ? false : this.applyAllowDelays(rawAllow);
        decision += `rawAllow=${rawAllow}, effectiveAllow=${effectiveAllow}; `;

        await this.setStateAck('status.activeMode', MODE_LABEL[control.mode] || 'unknown');
        await this.setStateAck('status.targetPhaseMode', targetPhaseMode);
        await this.setStateAck('calculation.formula', activeFormula);
        await this.setStateAck('calculation.availablePowerW', Number.isFinite(availablePowerW) ? Math.round(availablePowerW) : 0);
        await this.setStateAck('calculation.nonEvHouseConsumptionW', Math.round(nonEvHouseConsumptionW));
        await this.setStateAck('calculation.gridImportLimitExceededW', Math.round(gridImportLimitExceededW));
        await this.setStateAck('calculation.targetCurrentRawA', targetCurrentRawA);
        await this.setStateAck('calculation.targetCurrentFinalA', targetCurrentFinalA);
        await this.setStateAck('calculation.socLimitReached', socLimitReached);

        if (socLimitReached) {
            decision += `socLimitReached (${inputs.carSocPercent}% >= ${control.targetSocPercent}%); `;
        }

        if (!control.allowCharging) {
            decision += 'global allowCharging=false; ';
        }
        if (control.emergencyStop) {
            decision += 'emergencyStop=true (immediate stop); ';
        }
        if (freshness.stale && control.mode !== MODE.GRID_MANUAL) {
            decision += `staleInputs=${freshness.staleKeys.join('|')}; `;
        }
        if (this.config.maxGridImportW >= 0 && gridImportLimitExceededW > 0) {
            decision += `gridImportLimitExceededBy=${Math.round(gridImportLimitExceededW)}W; `;
        }
        if (control.simulationMode) {
            decision += 'simulationMode=true; ';
        }

        await this.setStateAck('status.decision', decision.trim());

        await this.applyChargerCommands({
            effectiveAllow,
            targetCurrentFinalA,
            targetPhaseMode,
            currentKey,
            mode: control.mode,
            simulationMode: control.simulationMode,
        });
    }

    async readControlStates() {
        const allowCharging = this.normalizeBoolean(await this.getStateValue('control.allowCharging', true), true);
        const emergencyStop = this.normalizeBoolean(await this.getStateValue('control.emergencyStop', false), false);
        const simulationMode = this.normalizeBoolean(await this.getStateValue('control.simulationMode', this.config.defaultSimulationMode), this.config.defaultSimulationMode);
        const mode = this.clampInt(await this.getStateValue('control.mode', this.config.defaultMode), this.config.defaultMode, MODE.PV_EXPORT, MODE.GRID_MANUAL);

        let minCurrentA = this.normalizeCurrentToStep(
            this.clampInt(await this.getStateValue('control.minCurrentA', this.config.minCurrentA), this.config.minCurrentA, 6, 32),
            6,
            32,
            'up',
        ) ?? DEFAULT_CURRENT_STEPS_A[0];
        let maxCurrentA = this.normalizeCurrentToStep(
            this.clampInt(await this.getStateValue('control.maxCurrentA', this.config.maxCurrentA), this.config.maxCurrentA, 6, 32),
            6,
            32,
            'down',
        ) ?? DEFAULT_CURRENT_STEPS_A[DEFAULT_CURRENT_STEPS_A.length - 1];
        if (minCurrentA > maxCurrentA) {
            maxCurrentA = minCurrentA;
        }

        const gridCurrentA = this.normalizeCurrentToStep(
            this.clampInt(await this.getStateValue('control.gridManual.currentA', this.config.defaultGridCurrentA), this.config.defaultGridCurrentA, 6, 32),
            minCurrentA,
            maxCurrentA,
            'nearest',
        ) ?? minCurrentA;
        const gridPhaseMode = this.clampInt(await this.getStateValue('control.gridManual.phaseMode', this.config.defaultGridPhaseMode), this.config.defaultGridPhaseMode, 0, 2);

        const targetSocEnabled = this.normalizeBoolean(await this.getStateValue('control.targetSocEnabled', this.config.defaultTargetSocEnabled), this.config.defaultTargetSocEnabled);
        const targetSocPercent = this.clampInt(await this.getStateValue('control.targetSocPercent', this.config.defaultTargetSocPercent), this.config.defaultTargetSocPercent, 1, 100);

        return {
            allowCharging,
            emergencyStop,
            simulationMode,
            mode,
            gridCurrentA,
            gridPhaseMode,
            minCurrentA,
            maxCurrentA,
            targetSocEnabled,
            targetSocPercent,
        };
    }

    async readInputValues() {
        const gridExport = await this.readForeignPositiveValueWithMeta(this.config.gridExportObjectId, 'gridExportW');
        const gridImport = await this.readForeignPositiveValueWithMeta(this.config.gridImportObjectId, 'gridImportW');
        const pvPower = await this.readForeignPositiveValueWithMeta(this.config.pvPowerObjectId, 'pvPowerW');
        const houseConsumption = await this.readForeignPositiveValueWithMeta(this.config.houseConsumptionObjectId, 'houseConsumptionW');
        const homeBatteryCharge = await this.readForeignPositiveValueWithMeta(this.config.homeBatteryChargeObjectId, 'homeBatteryChargeW');
        const homeBatteryDischarge = await this.readForeignPositiveValueWithMeta(this.config.homeBatteryDischargeObjectId, 'homeBatteryDischargeW');

        const carSoc = await this.readForeignRawValueWithMeta(this.config.carSocObjectId, 'carSocPercent');
        const carSocPercent = this.clampNumber(carSoc.value, 0, 0, 100);

        return {
            gridExportW: gridExport.value,
            gridImportW: gridImport.value,
            pvPowerW: pvPower.value,
            houseConsumptionW: houseConsumption.value,
            homeBatteryChargeW: homeBatteryCharge.value,
            homeBatteryDischargeW: homeBatteryDischarge.value,
            carSocPercent,
            _meta: {
                gridExport,
                gridImport,
                pvPower,
                houseConsumption,
                homeBatteryCharge,
                homeBatteryDischarge,
                carSoc,
            },
        };
    }

    evaluateInputFreshness(inputs, mode) {
        const maxAge = this.config.maxInputAgeSec;
        const staleKeys = [];
        let oldestAgeSec = 0;

        const required = [];
        if (mode === MODE.PV_EXPORT) {
            required.push(inputs._meta.gridExport, inputs._meta.gridImport);
            if (this.config.maxGridImportW >= 0) {
                required.push(inputs._meta.gridImport);
            }
        } else if (mode === MODE.PV_BATTERY_LAST) {
            required.push(inputs._meta.pvPower, inputs._meta.houseConsumption);
            if (this.config.maxGridImportW >= 0) {
                required.push(inputs._meta.gridImport);
            }
        }

        const seen = new Set();
        for (const item of required) {
            if (!item) {
                continue;
            }
            const key = item.key || item.id || 'unknown';
            if (seen.has(key)) {
                continue;
            }
            seen.add(key);

            if (!item.configured) {
                staleKeys.push(`${key}:not-configured`);
                oldestAgeSec = Math.max(oldestAgeSec, maxAge + 1);
                continue;
            }
            oldestAgeSec = Math.max(oldestAgeSec, item.ageSec);
            if (!item.exists) {
                staleKeys.push(`${key}:missing`);
                continue;
            }
            if (item.ageSec > maxAge) {
                staleKeys.push(`${key}:age=${item.ageSec}s`);
            }
        }

        return {
            stale: staleKeys.length > 0,
            staleKeys,
            oldestAgeSec,
        };
    }

    calculateDynamicPhaseMode(availablePowerW) {
        const up = this.config.phaseSwitchUpThresholdW;
        const down = this.getPhaseSwitchDownThreshold();
        const now = Date.now();
        const holdMs = this.config.phaseSwitchMinHoldSec * 1000;

        if (![1, 2].includes(this.runtime.phaseControl.stableMode)) {
            this.runtime.phaseControl.stableMode = 1;
        }

        let candidate = this.runtime.phaseControl.stableMode;
        if (availablePowerW >= up) {
            candidate = 2;
        } else if (availablePowerW <= down) {
            candidate = 1;
        }

        if (candidate !== this.runtime.phaseControl.stableMode) {
            if (this.runtime.phaseControl.candidateMode !== candidate) {
                this.runtime.phaseControl.candidateMode = candidate;
                this.runtime.phaseControl.candidateSinceMs = now;
            } else if (now - this.runtime.phaseControl.candidateSinceMs >= holdMs) {
                this.runtime.phaseControl.stableMode = candidate;
                this.runtime.phaseControl.candidateMode = null;
                this.runtime.phaseControl.candidateSinceMs = 0;
            }
        } else {
            this.runtime.phaseControl.candidateMode = null;
            this.runtime.phaseControl.candidateSinceMs = 0;
        }

        return this.runtime.phaseControl.stableMode;
    }

    applyAllowDelays(rawAllow) {
        const now = Date.now();
        const startMs = this.config.startDelaySec * 1000;
        const stopMs = this.config.stopDelaySec * 1000;

        if (this.runtime.allowControl.stableAllow === null || this.runtime.allowControl.stableAllow === undefined) {
            this.runtime.allowControl.stableAllow = !!rawAllow;
        }

        if (rawAllow && !this.runtime.allowControl.stableAllow) {
            if (!this.runtime.allowControl.startCandidateSinceMs) {
                this.runtime.allowControl.startCandidateSinceMs = now;
            }
            if (now - this.runtime.allowControl.startCandidateSinceMs >= startMs) {
                this.runtime.allowControl.stableAllow = true;
                this.runtime.allowControl.startCandidateSinceMs = 0;
                this.runtime.allowControl.stopCandidateSinceMs = 0;
            }
        } else if (!rawAllow && this.runtime.allowControl.stableAllow) {
            if (!this.runtime.allowControl.stopCandidateSinceMs) {
                this.runtime.allowControl.stopCandidateSinceMs = now;
            }
            if (now - this.runtime.allowControl.stopCandidateSinceMs >= stopMs) {
                this.runtime.allowControl.stableAllow = false;
                this.runtime.allowControl.startCandidateSinceMs = 0;
                this.runtime.allowControl.stopCandidateSinceMs = 0;
            }
        } else {
            this.runtime.allowControl.startCandidateSinceMs = 0;
            this.runtime.allowControl.stopCandidateSinceMs = 0;
        }

        return this.runtime.allowControl.stableAllow;
    }

    async applyChargerCommands(plan) {
        const { effectiveAllow, targetCurrentFinalA, targetPhaseMode, currentKey, mode, simulationMode } = plan;
        const usesHttpWrite = this.config.writeTransport === 'http';
        const hasHttpReadPath = this.config.readTransport === 'http' || this.config.readTransport === 'hybrid';

        if (!simulationMode && usesHttpWrite && hasHttpReadPath && !this.runtime.charger.connected) {
            const now = Date.now();
            if (now - this.runtime.command.lastHttpUnavailableLogAtMs >= 60000) {
                this.runtime.command.lastHttpUnavailableLogAtMs = now;
                this.log.warn('Skip charger write commands: HTTP connection currently unavailable (last status read failed)');
            }
            return;
        }

        if (mode === MODE.GRID_MANUAL) {
            await this.sendPhaseModeIfNeeded(targetPhaseMode, simulationMode);
            if (effectiveAllow) {
                await this.sendCurrentIfNeeded(targetCurrentFinalA, currentKey, simulationMode);
            }
            await this.sendAllowIfNeeded(effectiveAllow, simulationMode);
            return;
        }

        await this.sendPhaseModeIfNeeded(targetPhaseMode, simulationMode);
        if (effectiveAllow) {
            await this.sendCurrentIfNeeded(targetCurrentFinalA, currentKey, simulationMode);
        }
        await this.sendAllowIfNeeded(effectiveAllow, simulationMode);
    }

    async sendAllowIfNeeded(allow, simulationMode) {
        if (this.runtime.command.lastAllow === allow) {
            return;
        }
        await this.sendCommand('alw', allow ? 1 : 0, simulationMode);
        this.runtime.command.lastAllow = allow;
        this.runtime.charger.chargingAllowed = allow;
        await this.setStateAck('status.effectiveAllowCharging', allow);
    }

    async sendCurrentIfNeeded(currentA, currentKey, simulationMode) {
        if (this.runtime.command.lastCurrentA === currentA && this.runtime.command.lastCurrentKey === currentKey) {
            return;
        }
        await this.sendCommand(currentKey, currentA, simulationMode);
        this.runtime.command.lastCurrentA = currentA;
        this.runtime.command.lastCurrentKey = currentKey;
    }

    async sendPhaseModeIfNeeded(psm, simulationMode) {
        if (!this.config.enableApiV2) {
            return;
        }
        if (this.runtime.command.lastPhaseMode === psm) {
            return;
        }
        await this.sendCommand('psm', psm, simulationMode);
        this.runtime.command.lastPhaseMode = psm;
        this.runtime.charger.actualPhaseMode = psm;
        await this.setStateAck('status.actualPhaseMode', psm);
    }

    async sendCommand(key, value, simulationMode = false) {
        const now = Date.now();
        const sinceLast = now - this.runtime.command.lastSentAtMs;
        if (sinceLast < this.config.commandMinIntervalMs) {
            await this.delay(this.config.commandMinIntervalMs - sinceLast);
        }

        try {
            if (simulationMode) {
                this.log.info(`[SIMULATION] skip write: ${key}=${value}`);
            } else if (this.config.writeTransport === 'mqtt') {
                if (!this.mqttClient || !this.mqttClient.connected) {
                    throw new Error('MQTT command transport selected, but MQTT is not connected');
                }
                const topic = this.getMqttCommandTopic();
                const payload = `${key}=${value}`;
                await new Promise((resolve, reject) => {
                    this.mqttClient.publish(topic, payload, { qos: 0, retain: false }, err => {
                        if (err) reject(err);
                        else resolve();
                    });
                });
                this.log.debug(`MQTT command sent ${topic}: ${payload}`);
            } else {
                if (key === 'psm') {
                    await this.httpGetWithRetry(`/api/set?psm=${encodeURIComponent(value)}`, {}, `HTTP command ${key}=${value}`);
                } else {
                    await this.httpGetWithRetry(`/mqtt?payload=${encodeURIComponent(`${key}=${value}`)}`, {}, `HTTP command ${key}=${value}`);
                }
                this.log.debug(`HTTP command sent ${key}=${value}`);
            }

            this.runtime.command.lastSentAtMs = Date.now();
            const stamp = new Date(this.runtime.command.lastSentAtMs).toISOString();
            await this.setStateAck('status.lastCommand', simulationMode ? `SIMULATED ${key}=${value}` : `${key}=${value}`);
            await this.setStateAck('status.lastCommandAt', stamp);
        } catch (err) {
            const msg = `Command failed ${key}=${value}: ${err.message || err}`;
            this.log.warn(msg);
            await this.setStateAck('diagnostics.lastError', msg);
            if (this.isTransientNetworkError(err)) {
                this.runtime.charger.connected = false;
                await this.setStateAck('info.connection', false);
                await this.setStateAck('status.connection', false);
            }
            throw err;
        }
    }

    supportsAmx() {
        const fw = Number.parseFloat(String(this.runtime.charger.firmware || '0'));
        if (!Number.isFinite(fw)) {
            return true;
        }
        return fw > 33;
    }

    getPhaseSwitchDownThreshold() {
        return this.config.phaseSwitchUpThresholdW - this.config.phaseSwitchHysteresisW;
    }

    buildCurrentStepStateMap(steps) {
        const map = {};
        for (const step of steps) {
            map[String(step)] = `${step} A`;
        }
        return map;
    }

    async updateCurrentControlStateMeta(force = false) {
        try {
            const steps = this.getAllowedCurrentSteps();
            if (!steps.length) {
                return;
            }

            const signature = steps.join(',');
            if (!force && this.runtime.controlMeta.currentStepSignature === signature) {
                return;
            }
            this.runtime.controlMeta.currentStepSignature = signature;

            const min = steps[0];
            const max = steps[steps.length - 1];
            const states = this.buildCurrentStepStateMap(steps);

            const commonPatch = {
                min,
                max,
                states,
            };

            await this.extendObjectAsync('control.gridManual.currentA', { common: commonPatch });
            await this.extendObjectAsync('control.minCurrentA', { common: commonPatch });
            await this.extendObjectAsync('control.maxCurrentA', { common: commonPatch });
            await this.setStateAck('status.allowedCurrentStepsA', signature);

            let normalizedMin = this.normalizeCurrentToStep(await this.getStateValue('control.minCurrentA', this.config.minCurrentA), min, max, 'up') ?? min;
            let normalizedMax = this.normalizeCurrentToStep(await this.getStateValue('control.maxCurrentA', this.config.maxCurrentA), min, max, 'down') ?? max;
            if (normalizedMin > normalizedMax) {
                normalizedMax = normalizedMin;
            }

            const normalizedGrid = this.normalizeCurrentToStep(
                await this.getStateValue('control.gridManual.currentA', this.config.defaultGridCurrentA),
                normalizedMin,
                normalizedMax,
                'nearest',
            ) ?? normalizedMin;

            await this.setStateAck('control.minCurrentA', normalizedMin);
            await this.setStateAck('control.maxCurrentA', normalizedMax);
            await this.setStateAck('control.gridManual.currentA', normalizedGrid);
        } catch (err) {
            this.log.warn(`Current-step metadata update failed: ${err.message || err}`);
        }
    }

    extractCurrentStepsFromStatus(status) {
        const steps = [];
        for (let idx = 1; idx <= 5; idx++) {
            const key = `al${idx}`;
            if (status[key] !== undefined) {
                const value = Number.parseInt(String(status[key]), 10);
                if (Number.isFinite(value) && value >= 6 && value <= 32) {
                    steps.push(value);
                }
            }
        }
        return this.sanitizeCurrentSteps(steps);
    }

    sanitizeCurrentSteps(values) {
        const unique = [...new Set((values || [])
            .map(v => Number.parseInt(String(v), 10))
            .filter(v => Number.isFinite(v) && v >= 6 && v <= 32))];
        unique.sort((a, b) => a - b);
        return unique;
    }

    getAllowedCurrentSteps() {
        const chargerSteps = this.sanitizeCurrentSteps(this.runtime?.charger?.allowedCurrentsA || []);
        if (chargerSteps.length) {
            return chargerSteps;
        }
        return [...DEFAULT_CURRENT_STEPS_A];
    }

    normalizeCurrentToStep(targetA, minA, maxA, mode = 'nearest') {
        const target = Number.parseInt(String(targetA), 10);
        if (!Number.isFinite(target)) {
            return null;
        }

        const low = Math.min(minA, maxA);
        const high = Math.max(minA, maxA);
        const steps = this.getAllowedCurrentSteps().filter(step => step >= low && step <= high);
        if (!steps.length) {
            return null;
        }

        if (mode === 'down') {
            let selected = null;
            for (const step of steps) {
                if (step <= target) {
                    selected = step;
                }
            }
            return selected;
        }

        if (mode === 'up') {
            for (const step of steps) {
                if (step >= target) {
                    return step;
                }
            }
            return null;
        }

        let selected = steps[0];
        let bestDiff = Math.abs(selected - target);
        for (const step of steps) {
            const diff = Math.abs(step - target);
            if (diff < bestDiff || (diff === bestDiff && step < selected)) {
                selected = step;
                bestDiff = diff;
            }
        }
        return selected;
    }

    async readForeignPositiveValueWithMeta(id, key) {
        const info = await this.readForeignRawValueWithMeta(id, key);
        info.value = Math.max(0, info.value);
        return info;
    }

    async readForeignRawValueWithMeta(id, key) {
        if (!id) {
            return {
                key,
                id: '',
                configured: false,
                exists: false,
                value: 0,
                ageSec: 0,
            };
        }
        try {
            const state = await this.getForeignStateAsync(id);
            if (!state || state.val === null || state.val === undefined) {
                return {
                    key,
                    id,
                    configured: true,
                    exists: false,
                    value: 0,
                    ageSec: this.config.maxInputAgeSec + 1,
                };
            }
            const stampMs = Math.max(Number(state.ts) || 0, Number(state.lc) || 0);
            const ageSec = stampMs > 0 ? Math.floor((Date.now() - stampMs) / 1000) : this.config.maxInputAgeSec + 1;
            return {
                key,
                id,
                configured: true,
                exists: true,
                value: Number(state.val) || 0,
                ageSec,
            };
        } catch (err) {
            this.log.debug(`Foreign state read failed (${id}): ${err.message || err}`);
            return {
                key,
                id,
                configured: true,
                exists: false,
                value: 0,
                ageSec: this.config.maxInputAgeSec + 1,
            };
        }
    }

    async ensureStateDefault(id, def) {
        const state = await this.getStateAsync(id);
        if (!state || state.val === null || state.val === undefined) {
            await this.setStateAck(id, def);
        }
    }

    async setStateAck(id, val) {
        await this.setStateAsync(id, { val, ack: true });
    }

    async getStateValue(id, fallback) {
        const state = await this.getStateAsync(id);
        if (!state || state.val === null || state.val === undefined) {
            return fallback;
        }
        return state.val;
    }

    clampInt(value, fallback, min, max) {
        const n = Number.parseInt(String(value), 10);
        if (!Number.isFinite(n)) {
            return fallback;
        }
        return Math.min(max, Math.max(min, n));
    }

    clampNumber(value, fallback, min, max) {
        const n = Number(value);
        if (!Number.isFinite(n)) {
            return fallback;
        }
        return Math.min(max, Math.max(min, n));
    }

    normalizeBoolean(value, fallback = false) {
        if (value === null || value === undefined) {
            return !!fallback;
        }
        if (typeof value === 'boolean') {
            return value;
        }
        if (typeof value === 'number') {
            return value !== 0;
        }
        if (typeof value === 'string') {
            const v = value.trim().toLowerCase();
            if (['true', '1', 'yes', 'on'].includes(v)) {
                return true;
            }
            if (['false', '0', 'no', 'off', ''].includes(v)) {
                return false;
            }
        }
        return !!fallback;
    }

    round1(value) {
        return Math.round(value * 10) / 10;
    }

    round3(value) {
        return Math.round(value * 1000) / 1000;
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async onUnload(callback) {
        try {
            if (this.pollTimer) {
                this.clearInterval(this.pollTimer);
            }
            if (this.mqttClient) {
                this.mqttClient.end(true);
                this.mqttClient = null;
            }
            callback();
        } catch (err) {
            callback();
        }
    }
}

if (module.parent) {
    module.exports = options => new GoEGeminiAdapter(options);
} else {
    (() => new GoEGeminiAdapter())();
}
