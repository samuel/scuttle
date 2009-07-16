
import time
from analytics.django import analytics
from django.http import get_host

class AnalyticsMiddleware(object):
    def process_request(self, request):
        self.start_time = time.time()

    def process_response(self, request, response):
        host = get_host(request).lower() # host:port
        if host.endswith(':80'):
            host = host[:-3]

        attributes = dict(
            host = host,
            url = u"http://%s%s" % (host, request.path),
        )

        if getattr(request, 'site', None):
            attributes['site_id'] = request.site.id
        if getattr(request, 'host', None):
            attributes['host_id'] = request.host.id

        referrer = request.META.get('HTTP_REFERER')
        if referrer:
            attributes['referrer'] = referrer

        if getattr(self, 'start_time', None):
            dt = time.time() - self.start_time
            self.start_time = None
            attributes['request_time'] = dt

        # possible props: time_since_last_session, initial_referrer

        if getattr(request, 'user', None) and request.user.is_authenticated():
            attributes['user_id'] = str(request.user.id)
        else:
            attributes['user_id'] = hasttrrequest.COOKIES.get(settings.SESSION_COOKIE_NAME, None)

        analytics.record("page_view", **attributes)
