import os
from datetime import datetime
from typing import List

import pandas as pd
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator

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
    def validate_timestep(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace(' ', 'T'))
        except ValueError as exc:
            raise ValueError('timestep must be in ISO format') from exc
        return v


class DataManager:
    def __init__(self, filename: str):
        self.filename = filename
        self.records: List[dict] = []
        self.next_id = 1

    def load(self) -> None:
        if not os.path.exists(self.filename):
            self.records = []
            self.next_id = 1
            self.save()
            return

        try:
            df = pd.read_csv(self.filename)
        except Exception as e:
            raise RuntimeError(f"Не удалось прочитать {self.filename}: {e}") from e

        self.records = []
        for idx, row in df.iterrows():
            self.records.append({
                "id": idx + 1,
                "timestep": str(row["timestep"]),
                "consumption_eur": float(row["consumption_eur"]),
                "consumption_sib": float(row["consumption_sib"]),
                "price_eur": float(row["price_eur"]),
                "price_sib": float(row["price_sib"])
            })
        self.next_id = len(self.records) + 1

    def save(self) -> None:
        try:
            df = pd.DataFrame(self.records)
            if not df.empty:
                df = df.drop(columns=["id"])
            df.to_csv(self.filename, index=False)
        except Exception as e:
            raise RuntimeError(f"Не удалось сохранить {self.filename}: {e}") from e

    def get_all(self) -> List[dict]:
        return self.records

    def add(self, record: RecordCreate) -> dict:
        new_record = record.model_dump()
        new_record["id"] = self.next_id
        self.records.append(new_record)
        self.next_id += 1
        self.save()
        return new_record

    def delete(self, record_id: int) -> None:
        for i, rec in enumerate(self.records):
            if rec["id"] == record_id:
                del self.records[i]
                self.save()
                return
        raise HTTPException(status_code=404, detail="Запись не найдена")


data_manager = DataManager(DATA_FILE)


@app.on_event("startup")
async def startup_event():
    try:
        data_manager.load()
    except Exception as e:
        print(f"Ошибка загрузки данных: {e}")


@app.get("/records", response_model=List[Record])
async def get_records():
    return data_manager.get_all()


@app.post("/records", response_model=Record, status_code=status.HTTP_201_CREATED)
async def add_record(record: RecordCreate):
    try:
        return data_manager.add(record)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка сохранения: {e}")


@app.delete("/records/{record_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_record(record_id: int):
    try:
        data_manager.delete(record_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка удаления: {e}")