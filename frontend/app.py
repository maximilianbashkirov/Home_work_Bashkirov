import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Electricity Market Dashboard", layout="wide")
st.title("Рынок потребления электроэнергии РФ (Европейская & Азиатская части)")

if 'user_name' not in st.session_state:
    st.session_state.user_name = ""
if 'user_surname' not in st.session_state:
    st.session_state.user_surname = ""
if 'user_position' not in st.session_state:
    st.session_state.user_position = ""
if 'user_contact' not in st.session_state:
    st.session_state.user_contact = ""
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

st.sidebar.header("Данные пользователя")
st.session_state.user_name = st.sidebar.text_input("Имя", value=st.session_state.user_name)
st.session_state.user_surname = st.sidebar.text_input("Фамилия", value=st.session_state.user_surname)
st.session_state.user_position = st.sidebar.text_input("Должность", value=st.session_state.user_position)
st.session_state.user_contact = st.sidebar.text_input("Контакт (TG/телефон)", value=st.session_state.user_contact)

if st.sidebar.button("Войти"):
    if st.session_state.user_name and st.session_state.user_surname:
        st.session_state.logged_in = True
        st.sidebar.success("Успешный вход!")
    else:
        st.sidebar.error("Пожалуйста, заполните имя и фамилию")

if st.session_state.logged_in:
    st.sidebar.markdown(f"Статус: 🟢 Вошел в систему")
    if st.session_state.user_name or st.session_state.user_surname:
        st.sidebar.markdown(f"Текущий пользователь: {st.session_state.user_name} {st.session_state.user_surname}")
    if st.session_state.user_position:
        st.sidebar.markdown(f"Должность: {st.session_state.user_position}")
    if st.session_state.user_contact:
        st.sidebar.markdown(f"Контакт: {st.session_state.user_contact}")
else:
    st.sidebar.markdown(f"Статус: 🔴 Не в системе")

st.sidebar.divider()

@st.cache_data(ttl=1)
def fetch_data():
    try:
        response = requests.get(f"{API_URL}/records", timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка подключения к API: {e}")
        return None

def refresh_data():
    st.cache_data.clear()

data = fetch_data()

if data is None:
    st.warning("Не удалось загрузить данные. Убедитесь, что FastAPI запущен.")
    st.stop()

if not data:
    st.info("API вернул пустой список.")
    df = pd.DataFrame(columns=["id", "timestep", "consumption_eur", "consumption_sib", "price_eur", "price_sib"])
else:
    df = pd.DataFrame(data)
    expected_cols = ["id", "timestep", "consumption_eur", "consumption_sib", "price_eur", "price_sib"]
    missing = [col for col in expected_cols if col not in df.columns]
    if missing:
        st.error(f"В данных отсутствуют колонки: {missing}")
        st.write("Структура полученных данных:", df.head())
        st.stop()
    df = df[expected_cols]

st.subheader("Данные")
st.dataframe(df, use_container_width=True, hide_index=True)

st.subheader("Графики")
col1, col2 = st.columns(2)

with col1:
    fig1 = px.line(df, x="timestep", y=["consumption_eur", "consumption_sib"],
                   labels={"value": "Consumption", "timestep": "Time"},
                   title="Потребление (EUR и SIB)")
    fig1.update_layout(legend_title_text="Zone")
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.line(df, x="timestep", y=["price_eur", "price_sib"],
                   labels={"value": "Price", "timestep": "Time"},
                   title="Цены (EUR и SIB)")
    fig2.update_layout(legend_title_text="Zone")
    st.plotly_chart(fig2, use_container_width=True)

st.sidebar.header("Добавить запись")

with st.sidebar.form("add_form"):
    timestep = st.text_input("Временной интервал", value="2026-01-01 00:00")
    col1, col2 = st.columns(2)
    with col1:
        cons_eur = st.number_input("Потребление EUR", value=0.0, step=1.0)
        cons_sib = st.number_input("Потребление SIB", value=0.0, step=1.0)
    with col2:
        price_eur = st.number_input("Цена EUR", value=0.0, step=1.0)
        price_sib = st.number_input("Цена SIB", value=0.0, step=1.0)

    submitted = st.form_submit_button("Добавить")
    if submitted:
        payload = {
            "timestep": timestep,
            "consumption_eur": cons_eur,
            "consumption_sib": cons_sib,
            "price_eur": price_eur,
            "price_sib": price_sib
        }
        try:
            resp = requests.post(f"{API_URL}/records", json=payload)
            if resp.status_code == 201:
                st.sidebar.success("Запись добавлена!")
                refresh_data()
            else:
                st.sidebar.error(f"Ошибка {resp.status_code}: {resp.text}")
        except Exception as e:
            st.sidebar.error(f"Ошибка соединения: {e}")

st.sidebar.header("Удалить запись")

with st.sidebar.form("delete_form"):
    delete_id = st.number_input("ID записи для удаления", min_value=1, step=1)
    delete_submitted = st.form_submit_button("Удалить")
    if delete_submitted:
        try:
            resp = requests.delete(f"{API_URL}/records/{delete_id}")
            if resp.status_code == 204:
                st.sidebar.success(f"Запись с ID {delete_id} удалена!")
                refresh_data()
            elif resp.status_code == 404:
                st.sidebar.error(f"ID {delete_id} не найден")
            else:
                st.sidebar.error(f"Ошибка {resp.status_code}: {resp.text}")
        except Exception as e:
            st.sidebar.error(f"Ошибка соединения: {e}")
