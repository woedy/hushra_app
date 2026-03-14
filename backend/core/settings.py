"""
Django settings for core project.
"""

import os
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('SECRET_KEY', 'django-insecure-dev-key-change-in-production')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get('DEBUG', 'True') == 'True'

ALLOWED_HOSTS = os.environ.get('ALLOWED_HOSTS', '*').split(',')

# Application definition
INSTALLED_APPS = [
    'daphne',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'channels',
    'corsheaders',
    'scraper',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'core.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'core.wsgi.application'
ASGI_APPLICATION = 'core.asgi.application'

# Database
# https://docs.djangoproject.com/en/6.0/ref/settings/#databases
import dj_database_url

# Prefer PostgreSQL by default. If DATABASE_URL is explicitly provided, it wins.
POSTGRES_DEFAULT_URL = os.environ.get(
    'POSTGRES_DEFAULT_URL',
    'postgres://hushra_user:hushra_pass@localhost:5432/hushra_db'
)

DATABASES = {
    'default': dj_database_url.config(
        default=os.environ.get('DATABASE_URL', POSTGRES_DEFAULT_URL),
        conn_max_age=600,
    )
}

# Password validation
# https://docs.djangoproject.com/en/6.0/ref/settings/#auth-password-validators
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/6.0/topics/i18n/
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/6.0/howto/static-files/
STATIC_URL = 'static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'

# Default primary key field type
# https://docs.djangoproject.com/en/6.0/ref/settings/#default-auto-field
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# CORS settings
CORS_ALLOW_ALL_ORIGINS = DEBUG
CORS_ALLOWED_ORIGINS = os.environ.get('CORS_ALLOWED_ORIGINS', 'http://localhost:5173').split(',')

# Celery Configuration
CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = TIME_ZONE
CELERY_BEAT_SCHEDULE = {
    'orchestrate-spider-every-minute': {
        'task': 'scraper.tasks.orchestrate_spider',
        'schedule': 60.0,
    },
}


# Task jitter configuration (used by scraper.tasks.execute_ssn_lookup)
# Keep defaults modest for production safety and make them env-configurable.
CELERY_MIN_JITTER = float(os.environ.get('CELERY_MIN_JITTER', 0.2))
CELERY_MAX_JITTER = float(os.environ.get('CELERY_MAX_JITTER', 1.0))
if CELERY_MAX_JITTER < CELERY_MIN_JITTER:
    CELERY_MAX_JITTER = CELERY_MIN_JITTER

# Channels Configuration
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            "hosts": [os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')],
        },
    },
}

# REST Framework Configuration
REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
    'DEFAULT_PARSER_CLASSES': [
        'rest_framework.parsers.JSONParser',
    ],
}

# Hushra specific settings

# Scraper tuning knobs
HUSHRA_TOKEN_CACHE_TTL = int(os.environ.get('HUSHRA_TOKEN_CACHE_TTL', 3600))
HUSHRA_LASTNAME_DEPTH_THRESHOLD = int(os.environ.get('HUSHRA_LASTNAME_DEPTH_THRESHOLD', 6))
HUSHRA_LOOKUP_LIMIT_THRESHOLD = int(os.environ.get('HUSHRA_LOOKUP_LIMIT_THRESHOLD', 50))
HUSHRA_NO_CREDENTIAL_RETRY_SECONDS = int(os.environ.get('HUSHRA_NO_CREDENTIAL_RETRY_SECONDS', 300))
HUSHRA_AUTH_FAILED_RETRY_SECONDS = int(os.environ.get('HUSHRA_AUTH_FAILED_RETRY_SECONDS', 10))
HUSHRA_RATE_LIMIT_RETRY_SECONDS = int(os.environ.get('HUSHRA_RATE_LIMIT_RETRY_SECONDS', 300))
HUSHRA_CREDENTIAL_SOFT_LIMIT = int(os.environ.get('HUSHRA_CREDENTIAL_SOFT_LIMIT', 80))
HUSHRA_AUTH_FAILED_COOLDOWN_HOURS = int(os.environ.get('HUSHRA_AUTH_FAILED_COOLDOWN_HOURS', 2))
