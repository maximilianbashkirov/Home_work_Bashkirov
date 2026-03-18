1. скачайте весь репозиторий **Task\_02\_service** в удобную вам папку на ПК

2. установите зависимости: откройте терминал и выполните команду: 
- $pip install -r requirements.txt

3. перейдите в **backend** часть:
- $cd backend

4. убедитесь, что там есть файл **data.csv**

5. запустите сервер: 
- $python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000

6. зупустите **frontend** часть, для этого перейдите в **frontend:**
- $cd frontend

7. и запустите приложение:
- $python -m streamlit run app.py

8. Интерфейс откроется в браузере (обычно http://localhost:8501)
