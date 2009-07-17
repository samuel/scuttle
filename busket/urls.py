from django.conf import settings
from django.conf.urls.defaults import *

# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('busket',
    (r'^api/js/1/record/$', 'api.views.js.record'),

    # (r'^admin/', include(admin.site.urls)),
    (r'^static/(?P<path>.*)$', 'django.views.static.serve',
        {'document_root': settings.MEDIA_ROOT}),
)
