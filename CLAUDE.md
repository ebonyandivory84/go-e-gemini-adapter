# go-e Gemini Adapter — Projektanweisungen

## Projektkontext
- **Ziel**: ioBroker-Adapter für go-e Gemini Charger mit deterministischer PV-Ladelogik
- **Typ**: ioBroker Adapter (JavaScript, TypeScript geplant), `vehicle`, Modus: `daemon`, `compact: true`
- **Status**: v0.1.0, aktiv in Betrieb
- **GitHub**: https://github.com/ebonyandivory84/go-e-gemini-adapter
- **Auftraggeber**: Eigenprojekt

## Adapter-Metadaten (io-package.json)
- **Name**: `go-e-gemini-adapter`
- **Typ**: `vehicle`, `connectionType: local`, `dataSource: poll`
- **Abhängigkeit**: `js-controller >= 5.0.19`
- **Charger-Default-IP**: `192.168.44.110`

## Repo-Struktur
```
go-e adapter (Holländer)/
├── main.js                 ← Adapter-Einstiegspunkt + gesamte Logik
├── io-package.json
├── package.json
├── icon.jpg / icon.png
├── admin/
│   ├── jsonConfig.json     ← 38 Konfigurationsfelder (5 Tabs)
│   └── icon.png
├── assets/
│   ├── go-e.png            ← Architektur-Diagramm
│   └── Settings.png        ← Screenshot Admin-UI
└── docs/screenshots/
    └── README.md
```

## Betriebsmodi

| Modus | `control.mode` | Logik |
|---|---|---|
| PV only | `1` | `availablePowerW = pvPowerW - (houseConsumptionW - chargerPowerW) - reservePowerW`; Akku muss voll sein (`homeBatterySoc >= batteryFullSocPercent`) |
| PV only (go-e priority) | `2` | Gleiche Formel, kein Akku-Voll-Kriterium |
| Grid mode | `3` | Direkter Sollwert: `control.gridManual.currentA` + `control.gridManual.phaseMode` |

Zusatzbedingungen Modi 1+2:
- `gridImportW <= pvOnlyFlowBufferW`
- `homeBatteryDischargeW <= pvOnlyFlowBufferW`
- Bei `maxGridImportW` aktiv: Überschreitung direkt von `availablePowerW` abziehen

## Freigabe-Logik
```
effectiveAllow = emergencyStop ? false : delayed(rawAllow)
```
- `emergencyStop` hat immer Priorität — sofortiger Stopp ohne Delay
- `rawAllow` = alle fachlichen Bedingungen (Modus, Leistung, SoC, Freshness, Buffer)
- Start/Stop-Verzögerung: `startDelaySec` / `stopDelaySec` gegen Flattern

## Admin-Konfigurationsfelder (5 Tabs, 38 Felder)

### Tab: Verbindung
| Feld | Typ | Bedeutung |
|---|---|---|
| `chargerHost` | text | IP/Hostname des Chargers |
| `pollIntervalSec` | number | Poll-Intervall in Sekunden |
| `httpTimeoutMs` | number | HTTP-Timeout in ms |
| `enableApiV2` | checkbox | API v2 aktivieren (z.B. für `psm`/Phasenmodus) |
| `readTransport` | select | `http` / `mqtt` / `hybrid` |
| `writeTransport` | select | `http` / `mqtt` |

### Tab: MQTT
| Feld | Typ |
|---|---|
| `mqttBrokerUrl` | text |
| `mqttUsername` | text |
| `mqttPassword` | password |
| `mqttTopicPrefix` | text (Standard: `go-eCharger`) |
| `mqttSerial` | text (Seriennummer im Topic) |

### Tab: Eingangsdaten (alle `objectId`)
| Feld | Bedeutung |
|---|---|
| `gridExportObjectId` | Netzeinspeisung [W] |
| `gridImportObjectId` | Netzbezug [W] |
| `pvPowerObjectId` | Aktuelle PV-Leistung [W] |
| `houseConsumptionObjectId` | Aktueller Hausverbrauch [W] |
| `homeBatteryChargeObjectId` | Hausakku Ladeleistung [W] |
| `homeBatteryDischargeObjectId` | Hausakku Entladeleistung [W] |
| `homeBatterySocObjectId` | Hausakku Ladezustand [%] |
| `carSocObjectId` | Auto-Batterie SoC [%] |

Hinweis: `gridExport` + `homeBatteryCharge` werden gespiegelt, sind aber nicht Teil der Freigabeformel.

### Tab: Regelung / Hysterese
| Feld | Bedeutung |
|---|---|
| `reservePowerW` | Puffer zwischen PV-Ertrag und Nutzung |
| `phaseSwitchUpThresholdW` | Schwelle für Phasenwechsel nach oben |
| `phaseSwitchHysteresisW` | Hysterese Phasenwechsel |
| `phaseSwitchMinHoldSec` | Mindesthaltezeit nach Phasenwechsel |
| `startDelaySec` | Startverzögerung |
| `stopDelaySec` | Stoppverzögerung |
| `currentRampDownHoldSec` | Haltezeit vor Reduktion des Ladestroms (nur PV-Modi); Erhöhungen sofort, Reduktion erst nach durchgängiger Haltezeit |
| `sessionStopGraceSec` | Gnadenfrist, bevor eine Unterschreitung der Stop-Schwelle (80W) die Session wirklich beendet (verhindert Session-Reset durch kurze PV-Ladepausen) |
| `maxInputAgeSec` | Maximales Alter der Eingangsdaten (Freshness) |
| `maxGridImportW` | Max. Netzbezug (-1 = deaktiviert) |
| `pvOnlyFlowBufferW` | Toleranzpuffer für Grid/Entladung |
| `batteryFullSocPercent` | SoC-Schwelle "Akku voll" (Modus 1) |
| `minCurrentA` / `maxCurrentA` | Strombegrenzung |
| `commandMinIntervalMs` | Mindestabstand zwischen Befehlen |

### Tab: Startwerte
| Feld | Bedeutung |
|---|---|
| `defaultMode` | Start-Betriebsmodus |
| `defaultGridCurrentA` | Start-Strom im Netz-Modus |
| `defaultGridPhaseMode` | Start-Phasenmodus im Netz-Modus |
| `defaultTargetSocEnabled` | SoC-Limit standardmäßig aktiv |
| `defaultTargetSocPercent` | SoC-Limit-Wert [%] |
| `defaultSimulationMode` | Simulation (Dry-Run) beim Start aktiv |

## State-Baum (ioBroker)

### Steuerung `control.*` (schreibbar)
- `control.allowCharging` — Master-Switch
- `control.emergencyStop` — Sofortstopp (Priorität vor allem)
- `control.simulationMode` — Dry-Run
- `control.mode` — Betriebsmodus (1/2/3)
- `control.gridManual.currentA` / `control.gridManual.phaseMode` — Modus 3
- `control.minCurrentA` / `control.maxCurrentA`
- `control.targetSocEnabled` / `control.targetSocPercent`

### Status `status.*` (lesbar)
- `status.connection`
- `status.activeMode`
- `status.effectiveAllowCharging`
- `status.targetPhaseMode` / `status.actualPhaseMode`
- `status.chargerPowerW` / `status.chargerCurrentA`
- `status.setCurrentA` / `status.setCurrentVolatileA`
- `status.lastCommand` / `status.lastCommandAt`
- `status.sessionActive` / `status.sessionEnergyWh` / `status.sessionEnergyKWh`
- `status.decision` — **wichtigster Debug-State** (Trigger + alle Blockgründe)

### Diagnostik `diagnostics.*`
- `diagnostics.lastError`
- `diagnostics.inputsStale` / `diagnostics.staleInputList`
- `diagnostics.oldestInputAgeSec`
- `diagnostics.httpReadFailStreak`
- `diagnostics.readSource`

## Transport-Details

### HTTP
| Aktion | Endpunkt |
|---|---|
| Status lesen v1 | `GET /status` |
| Status lesen v2 | `GET /api/status` (gefiltert) |
| Befehl schreiben v1 | `GET /mqtt?payload=key=value` |
| Phasenmodus v2 | `GET /api/set?psm=...` |

### MQTT
- Status-Topic: `<prefix>/<serial>/status`
- Command-Topic: `<prefix>/<serial>/cmd/req`

## Debug-Workflow
1. `status.decision` lesen → zeigt Trigger + alle Blockgründe
2. `diagnostics.inputsStale` + `diagnostics.staleInputList` prüfen
3. `status.effectiveAllowCharging` mit `control.allowCharging` + `emergencyStop` vergleichen
4. `status.lastCommand` / `status.lastCommandAt` auf gesendete Befehle prüfen

## Typische Fehlerursachen
| Symptom | Ursache | Fix |
|---|---|---|
| Lädt nicht trotz PV | Buffer-, SoC- oder Freshness-Blocker | `status.decision` prüfen |
| Keine Befehle am Charger | MQTT-Verbindung / HTTP-Erreichbarkeit | `status.transportWrite` prüfen |
| Werte "springen" | Delays zu kurz | `startDelaySec`, `stopDelaySec`, `phaseSwitchMinHoldSec`, `currentRampDownHoldSec` erhöhen |
| Unerwarteter Netzbezug | Reserve zu klein | `reservePowerW` erhöhen, `maxGridImportW` aktivieren |

## Zuständige Skills
| Aufgabe | Tool |
|---|---|
| Adapter-Logik (JS) | `ecc:typescript-reviewer` |
| Architekturentscheidungen | `ecc:architect` |
| Sicherheitsrelevante Änderungen | `ecc:security-reviewer` |
| Struktur visualisieren | `graphify .` (nur JS-Dateien, kein API-Key nötig) |
