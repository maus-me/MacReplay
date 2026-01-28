# MacReplay - Ideen & Roadmap

Eine Sammlung von Verbesserungsvorschlägen und Feature-Ideen für zukünftige Entwicklung.

---

## Legende

- [ ] Offen
- [x] Erledigt
- [~] In Arbeit

---

## UI/UX Verbesserungen

### Dashboard / Übersichtsseite
- [ ] Neue Startseite mit Gesamtübersicht
- [ ] Widget: Aktive Portale, MACs, Channels Statistik
- [ ] Widget: MACs die in 7/30 Tagen ablaufen
- [ ] Widget: Zuletzt genutzte Channels
- [ ] Widget: System-Status (EPG aktuell, letzte Fehler)

### Suche & Filter
- [ ] Globale Suche über Portale, MACs und Channels
- [ ] Channels-Seite: Filter "Nur Channels ohne EPG"
- [ ] Portals-Seite: Filter "Nur ablaufende MACs"
- [ ] Channels-Seite: Favoriten markieren (Stern-Icon)

### Bulk-Operationen
- [ ] Mehrere MACs gleichzeitig auswählen und löschen
- [ ] Portal-Konfiguration exportieren (JSON)
- [ ] Portal-Konfiguration importieren
- [ ] Channels zwischen Gruppen verschieben (Drag & Drop)

### Genre/Gruppen-Auswahl beim Portal-Hinzufügen
- [x] **Genre-Auswahl beim Hinzufügen eines Portals**
  - 2-Schritt Wizard: Erst Portal-Info, dann Genre-Checkboxen
  - API-Endpunkt `/api/portal/genres` holt verfügbare Genres vom Portal
  - ✅ Implementiert: Nur ausgewählte Genres werden beim Channel-Sync importiert

- [x] **Genre-Auswahl beim Bearbeiten eines Portals**
  - Button "Load Genres" im Edit-Modal
  - Bestehende Genre-Auswahl wird beibehalten
  - ✅ Implementiert: Genre-Filter kann jederzeit geändert werden

### Portal-Import aus Textdateien
- [ ] Import von Portalen aus formatierten Scan-Ergebnis-Dateien
- [ ] Automatische Erkennung von Portal-URL und MAC-Adresse
- [ ] Optional: Ablaufdatum extrahieren
- [ ] Mehrere Portale pro Datei unterstützen
- [ ] Preview vor Import (welche Portale erkannt wurden)
- [ ] Duplikat-Erkennung (Portal+MAC bereits vorhanden)

**Beispiel-Eingabeformat:**
```
🌐 Panel     ➤ http://example.com:80/c/
🔢 MAC Addr  ➤ 00:1A:79:2D:24:01
📆 Expiração = 24-04-2026 [89 dias]
```

**Erkennungs-Patterns:**
```python
patterns = {
    'url': r'(?:Panel|Server|URL|Host)\s*[➤:=]\s*(https?://[^\s]+)',
    'mac': r'(?:MAC\s*Addr|MAC)\s*[➤:=]\s*([0-9A-Fa-f:]{17})',
    'expiry': r'(?:Expir|Ablauf|Exp)\w*\s*[=:]\s*(\d{2}-\d{2}-\d{4})',
}
```

### Allgemeine UI
- [ ] Responsive Design für Mobile verbessern
- [ ] Tastatur-Shortcuts (z.B. `/` für Suche)
- [ ] Sortierung der Portale per Drag & Drop
- [ ] Kompakte Ansicht für Channel-Liste

---

## Benachrichtigungen

### MAC-Ablauf-Warnungen
- [ ] E-Mail-Benachrichtigung X Tage vor Ablauf
- [ ] Konfigurierbare Warnschwellen (z.B. 30, 14, 7, 1 Tag)
- [ ] Webhook-Support für externe Services
  - [ ] Discord
  - [ ] Telegram
  - [ ] Slack
  - [ ] Generic Webhook (POST JSON)
- [ ] Browser Push-Notifications

### System-Benachrichtigungen
- [ ] Warnung wenn Portal nicht erreichbar
- [ ] Benachrichtigung bei EPG-Fehler
- [ ] Info wenn neue Channels verfügbar sind
- [ ] Tägliche/Wöchentliche Zusammenfassung per E-Mail

---

## Analytics & Monitoring

### Erweiterte MAC-Informationen (inspiriert von macreplay)
Die Stalker Portal API liefert via `?type=stb&action=get_profile` zusätzliche Informationen:

- [x] **Max Streams anzeigen** (`playback_limit`)
  - Zeigt wie viele gleichzeitige Streams pro MAC erlaubt sind
  - In der MAC-Übersicht als Spalte darstellen
  - ✅ Implementiert: Wird beim Hinzufügen/Aktualisieren eines Portals abgerufen und in der MAC-Tabelle angezeigt

- [x] **Watchdog Timeout anzeigen** (`watchdog_timeout`)
  - Zeigt Sekunden seit letzter Aktivität der MAC-Adresse
  - Interpretation:
    - < 60s = Sehr aktiv (🔴 gerade am Streamen)
    - 60-300s = Aktiv (🟡 kürzlich benutzt)
    - 300-1800s = Moderate Aktivität (🔵)
    - > 1800s = Idle (🟢 sicher zu benutzen)
  - ✅ Implementiert: Farbkodierte Badges in der MAC-Übersicht

- [ ] **MAC-Status-Prüfung**
  - Button "Status prüfen" für einzelne MACs oder alle MACs eines Portals
  - Zeigt: Watchdog, Max Streams, Account-Status, Ablaufdatum
  - Hilft zu erkennen, ob eine MAC gerade von jemand anderem genutzt wird

- [x] **Intelligente MAC-Auswahl**
  - Automatisch die "beste" MAC für einen Stream wählen
  - ✅ Implementiert: Scoring basierend auf Watchdog (idle bevorzugen) und verfügbare Streams
  - MACs werden vor dem Streaming nach Score sortiert

**Referenz:** Siehe `/host_opt/macreplay/stb.py` Zeilen 1522-1732 für Implementation

### Nutzungsstatistiken
- [ ] Channel-Popularität tracken (Aufrufe zählen)
- [ ] Bandbreitenverbrauch pro Portal/MAC
- [ ] Stream-Uptime pro Channel
- [ ] Grafiken mit Chart.js oder ähnlich

### History & Logs
- [ ] MAC-Änderungs-Historie (wann hinzugefügt/gelöscht)
- [ ] Erweitertes Log-Viewing mit Filter
- [ ] API-Zugriffs-Log
- [ ] Log-Export als Datei

---

## Technische Erweiterungen

### Multi-User Support
- [ ] Benutzerregistrierung und Login
- [ ] Rollen-System (Admin, Editor, Viewer)
- [ ] Portale bestimmten Benutzern zuweisen
- [ ] Audit-Log für alle Änderungen
- [ ] Session-Management

### API-Erweiterungen
- [ ] REST API mit Authentifizierung
- [ ] API-Dokumentation (OpenAPI/Swagger)
- [ ] Prometheus Metrics Endpoint `/metrics`
- [ ] Health-Check Endpoint `/health`
- [ ] Rate-Limiting für API

### Backup & Restore
- [ ] Manuelles Backup erstellen (Button in Settings)
- [ ] Automatische Backups (täglich/wöchentlich)
- [ ] Backup-Rotation (nur X Backups behalten)
- [ ] One-Click Restore
- [ ] Backup-Download als verschlüsselte Datei

### Performance
- [ ] Redis-Cache für häufige Abfragen
- [ ] Channel-Logo Caching lokal
- [ ] Lazy-Loading für große Channel-Listen
- [ ] Database Connection Pooling

---

## Streaming-Features

### Aufnahme / DVR
- [ ] EPG-basierte Aufnahmeplanung
- [ ] Aufnahme-Manager UI
- [ ] Speicherort konfigurierbar
- [ ] Automatisches Löschen alter Aufnahmen

### Stream-Qualität
- [ ] Qualitätsauswahl pro Channel (wenn verfügbar)
- [ ] Transkodierung für schwache Verbindungen
- [ ] Adaptive Bitrate Streaming

### Wiedergabe
- [ ] Integrierter Web-Player
- [ ] Timeshift-Funktion
- [ ] Catch-up TV Support

---

## Channel-Management

### Channel-Name Normalisierung
- [ ] Länder-Tags entfernen oder vereinheitlichen (z.B. `[DE]`, `DE:`, `🇩🇪`)
- [ ] Qualitäts-Tags normalisieren (HD, FHD, 4K, UHD → einheitliches Format)
- [ ] Unnötige Sonderzeichen und Leerzeichen entfernen
- [ ] Regelbasiertes System für Normalisierung (konfigurierbar)
- [ ] Preview vor Anwendung der Normalisierung

**Offene Fragen:**
- Wie Normalisierung konsistent halten, wenn Channels regelmäßig vom Portal aktualisiert werden?
  - Möglichkeit: Mapping-Tabelle (Original-Name → Normalisierter Name)
  - Möglichkeit: Normalisierung bei jedem Sync automatisch anwenden
- Wie EPG-Zuordnung trotz geänderter Namen sicherstellen?
  - Möglichkeit: EPG-Mapping über Channel-ID statt Name
  - Möglichkeit: Fuzzy-Matching für EPG-Zuordnung

### Event-Channels (EPG-basiert)
- [ ] Channels automatisch aus EPG-Einträgen generieren
- [ ] Mehrere Events pro Quell-Channel → mehrere Event-Channels
- [ ] Kein EPG für Event-Channels nötig (Name = Info)
- [ ] Konfigurierbare Regeln (welche Channels, welche Event-Typen)
- [ ] Automatische Löschung nach Event-Ende

**EPG-Muster Beispiele:**
```
Eishockey:
  Titel: "LIVE: Augsburger Panther - Eisbären Berlin"
  Text:  "Augsburger Panther - Eisbären Berlin, PENNY DEL, Spieltag 44"

Fußball:
  Titel: "Live BL: Werder Bremen - TSG Hoffenheim, Nachholspiel vom 16. Spieltag"
```

**Regel-Konfiguration (Konzept):**
```yaml
event_rules:
  - name: "Bundesliga"
    channels:
      - "Sky Sport Bundesliga*"
      - "Sky Sport Top Event"
    pattern: "Live.*BL:|Bundesliga"
    extract: "(?P<home>.+?) - (?P<away>.+?),"
    output: "{home} vs {away} | {date} {time}"

  - name: "DEL Eishockey"
    channels:
      - "Sport1*"
      - "MagentaSport*"
    pattern: "LIVE:.*DEL|PENNY DEL"
    extract: "(?P<home>.+?) - (?P<away>.+)"
    output: "{home} vs {away} | DEL | {date} {time}"
```

**Generiertes Ergebnis:**
```
Original-Channel: Sky Sport Bundesliga 1
EPG-Eintrag:      "Live BL: Werder Bremen - TSG Hoffenheim" @ 15:30

→ Event-Channel:  "Werder Bremen vs Hoffenheim | 27.01 15:30"
                  (verlinkt auf Sky Sport Bundesliga 1)
```

### Automatische Backup-Channels
- [ ] Channels mit gleichem (normalisierten) Namen erkennen
- [ ] Automatisch als Backup-Gruppe zusammenfassen
- [ ] Failover bei Stream-Ausfall zum nächsten Backup
- [ ] Priorität per Drag & Drop festlegen

**Offene Fragen:**
- Automatisches Probing mit ffmpeg/ffprobe bei vielen Channels zu aufwendig?
  - Möglichkeit: Nur bei Wiedergabe-Start proben
  - Möglichkeit: Hintergrund-Job mit Rate-Limiting
  - Möglichkeit: Nur manuell ausgelöstes Probing

---

## Infrastruktur

### Docker
- [ ] Multi-Arch Images (ARM64 für Raspberry Pi)
- [ ] Docker Healthcheck verbessern
- [ ] Docker Compose Beispiele erweitern
- [ ] Kubernetes Helm Chart

### Deployment
- [ ] SSL/TLS Konfiguration vereinfachen
- [ ] Reverse Proxy Dokumentation
- [ ] One-Click Deploy für populäre Plattformen

---

## Dokumentation

- [ ] Benutzerhandbuch
- [ ] API-Dokumentation
- [ ] Entwickler-Setup Guide
- [ ] FAQ / Troubleshooting
- [ ] Video-Tutorials

---

## Priorisierte Roadmap

### Phase 1 - Quick Wins
1. Dashboard mit Übersicht
2. Globale Suche
3. MAC-Ablauf E-Mail-Benachrichtigungen

### Phase 2 - Core Features
4. Backup & Restore
5. Multi-User Support (Basic)
6. Webhook-Benachrichtigungen

### Phase 3 - Advanced
7. REST API
8. Analytics Dashboard
9. Aufnahme-Funktion

---

## Beitragen

Ideen und Vorschläge sind willkommen! Erstelle ein Issue oder PR auf GitHub.
