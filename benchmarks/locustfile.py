from locust import HttpUser, task, between

PAYLOAD = {"prompt": "Explain photosynthesis briefly.", "max_tokens": 100}


class InferenceUser(HttpUser):
    wait_time = between(0.5, 2)

    @task
    def generate(self):
        with self.client.post("/generate", json=PAYLOAD, catch_response=True, name="/generate") as resp:
            if resp.status_code != 200:
                meta = getattr(resp, "request_meta", None)
                if meta is not None:
                    meta["name"] = "/generate (failed)"
                err = getattr(resp, "error", None)
                resp.failure(repr(err) if err else f"HTTP {resp.status_code}")