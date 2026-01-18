# Railway busca main.py por defecto
# Este archivo importa la app desde app.py

from app import app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
