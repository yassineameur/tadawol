web: uvicorn api:app --host=0.0.0.0 --port=${PORT:-5000} --reload
worker: celery -A tasks worker --loglevel=info
