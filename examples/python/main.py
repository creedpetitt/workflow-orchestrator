from workersdk import Worker

def main():
    worker = Worker()

    worker.register("step1", lambda payload: f"step1-result: processed {payload}")
    worker.register("step2", lambda payload: f"step2-result: finished with {payload}")

    worker.start("postgresql://admin:admin@postgres:5432/wf_engine")

if __name__ == "__main__":
    main()
