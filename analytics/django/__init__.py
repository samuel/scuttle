
# Create a default analytics instance

def create_analytics():
    from django.conf import settings
    logger_class_path, logger_args = settings.ANALYTICS_LOGGER
    class_path = logger_class_path.rsplit('.', 1)
    logger_class = getattr(__import__(class_path[0], {}, {}, [class_path[1]]), class_path[1])
    logger = logger_class(**logger_args)
    return Analytics(logger)

analytics = create_analytics()
