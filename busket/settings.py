# Django settings for zalytics project.

import os.path, sys
PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(PROJECT_PATH)
if PROJECT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_PATH)
if BASE_PATH not in sys.path:
    sys.path.insert(0, BASE_PATH)

DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)

MANAGERS = ADMINS

DATABASE_ENGINE = ''           # 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
DATABASE_NAME = ''             # Or path to database file if using sqlite3.
DATABASE_USER = ''             # Not used with sqlite3.
DATABASE_PASSWORD = ''         # Not used with sqlite3.
DATABASE_HOST = ''             # Set to empty string for localhost. Not used with sqlite3.
DATABASE_PORT = ''             # Set to empty string for default. Not used with sqlite3.

TIME_ZONE = 'GMT'
LANGUAGE_CODE = 'en-us'
SITE_ID = 1
USE_I18N = False

ANALYTICS_LOGGER = ("analytics.loggers.TestLogger", {})

MEDIA_ROOT = os.path.join(PROJECT_PATH, "static")
MEDIA_URL = '/static/'
ADMIN_MEDIA_PREFIX = '/static/admin/'

SECRET_KEY = 'kryj8nu)ubhn0jgmc5mmvp52&#bt+dw7krt&-88wg^%_#k@ywj'

TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
#     'django.template.loaders.eggs.load_template_source',
)

MIDDLEWARE_CLASSES = (
    # 'django.middleware.common.CommonMiddleware',
)

ROOT_URLCONF = 'busket.urls'

TEMPLATE_DIRS = (
    os.path.join(PROJECT_PATH, "templates"),
)

INSTALLED_APPS = (
    # 'main',
)

try:
    from local_settings import *
except ImportError:
    pass
