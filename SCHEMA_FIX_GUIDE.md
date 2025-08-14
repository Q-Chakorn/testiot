# Database Schema Fix Guide

## ปัญหาที่เจอ
- `relation "raw_data" does not exist`
- `ON CONFLICT specification` ไม่ตรงกัน
- Application ใช้ `value` column แต่ schema ใช้ `raw_value`

## วิธีแก้ไขถาวร

### 1. Files ที่แก้ไขแล้ว
- ✅ `datalogger-agent/schema.sql` - แก้ไข schema ให้ถูกต้อง
- ✅ `datalogger-agent/docker-init-db.sh` - init script ที่แก้ปัญหา
- ✅ `datalogger-agent/fix-schema-migration.sql` - migration script สำหรับ database เก่า

### 2. การตั้งค่า TimescaleDB (2.timescaledb.yaml)
```yaml
volumes:
  - ./datalogger-agent/init_db.sql:/docker-entrypoint-initdb.d/01-init_db.sql:ro
  - ./datalogger-agent/schema.sql:/docker-entrypoint-initdb.d/02-schema.sql:ro
  - ./datalogger-agent/docker-init-db.sh:/docker-entrypoint-initdb.d/03-init-script.sh:ro
```

### 3. Schema ที่ถูกต้อง
- Table: `raw_data` 
- Primary Key: `(datetime, device_id, datapoint)`
- ON CONFLICT constraint: `UNIQUE (datetime, device_id, datapoint)`
- Columns: `id, timestamp, datetime, device_id, datapoint, value, created_at`

### 4. การทดสอบ
```bash
# Reset database (ถ้าจำเป็น)
docker stop datalogger-timescaledb
docker rm datalogger-timescaledb
docker volume rm datalogger-timescaledb-data
docker volume create datalogger-timescaledb-data

# Start fresh database
docker compose -f 2.timescaledb.yaml up -d

# Restart datalogger
docker restart datalogger-agent

# Check logs
docker logs datalogger-agent --tail 5
```

## ผลลัพธ์
- ✅ Database schema ถูกต้อง
- ✅ ON CONFLICT ทำงานได้
- ✅ Data insert สำเร็จ: "Batch insert successful: X rows affected"
- ✅ ระบบทำงานปกติ

## การป้องกันปัญหาในอนาคต
1. ใช้ persistent volume สำหรับ TimescaleDB
2. มี init scripts ที่ถูกต้องใน `/docker-entrypoint-initdb.d/`  
3. Test schema ก่อน deploy production
4. มี migration scripts สำหรับ schema changes

## หมายเหตุ
- Schema นี้แก้ไขให้ตรงกับ DataLogger Agent application
- ใช้ได้กับ `testiotacr.azurecr.io/datalogger-agent:v1` และ versions ใหม่ๆ
- เวลา restart/rebuild จะไม่เจอปัญหาเดิมอีก
