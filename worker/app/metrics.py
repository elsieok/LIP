from prometheus_client import Counter, Histogram, Gauge, make_asgi_app

REQUEST_COUNT = Counter("inference_requests_total", "Total requests received")
REQUEST_LATENCY = Histogram("inference_request_latency_seconds", "End-to-end request latency", buckets=(0.5, 1, 2, 5, 10, 20, 30, 60, 120))
CONSUMER_LAG = Gauge("kafka_consumer_lag", "Current consumer lag on inference-requests")

metrics_app = make_asgi_app()