import pandas as pd
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, field_validator
from typing import List
import os
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Electricity Market API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_FILE = "data.csv"

class Record(BaseModel):
    id: int
    timestep: str
    consumption_eur: float
    consumption_sib: float
    price_eur: float
    price_sib: float

class RecordCreate(BaseModel):
    timestep: str
    consumption_eur: float
    consumption_sib: float
    price_eur: float
    price_sib: float

    @field_validator('timestep')
    def validate_timestep(cls, v):
        try:
            datetime.fromisoformat(v.replace(' ', 'T'))
        except ValueError:
            raise ValueError('timestep must be in ISO format')
        return v

records: List[dict] = []
next_id: int = 1

def load_data():
    global records, next_id
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Файл с данными {DATA_FILE} не найден")
    df = pd.read_csv(DATA_FILE)
    records.clear()
    for idx, row in df.iterrows():
        records.append({
            "id": idx + 1,
            "timestep": str(row["timestep"]),
            "consumption_eur": float(row["consumption_eur"]),
            "consumption_sib": float(row["consumption_sib"]),
            "price_eur": float(row["price_eur"]),
            "price_sib": float(row["price_sib"])
        })
    next_id = len(records) + 1

def save_data():
    df = pd.DataFrame(records)
    df.drop(columns=["id"], inplace=True)
    df.to_csv(DATA_FILE, index=False)

@app.on_event("startup")
async def startup_event():
    load_data()

@app.get("/records", response_model=List[Record])
async def get_records():
    return records

@app.post("/records", response_model=Record, status_code=status.HTTP_201_CREATED)
async def add_record(record: RecordCreate):
    global next_id
    new_record = record.model_dump()
    new_record["id"] = next_id
    records.append(new_record)
    next_id += 1
    save_data()
    return new_record

@app.delete("/records/{record_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_record(record_id: int):
    for i, rec in enumerate(records):
        if rec["id"] == record_id:
            del records[i]
            save_data()
            return
    raise HTTPException(status_code=404, detail="Не найдено")
