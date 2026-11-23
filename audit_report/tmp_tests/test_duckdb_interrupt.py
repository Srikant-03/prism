import duckdb
import threading
import time

conn = duckdb.connect(database=":memory:", config={"enable_external_access": "false", "max_memory": "2GB"})

def interrupt_test():
    timer = threading.Timer(0.5, conn.interrupt)
    timer.start()
    try:
        # A slow query, e.g. cross join or generate_series
        conn.execute("SELECT COUNT(*) FROM generate_series(1, 1000000000) t1, generate_series(1, 1000000000) t2")
        print("Query finished unexpectedly")
    except duckdb.InterruptException as e:
        print("Successfully interrupted by Timer:", e)
    except Exception as e:
        print("Interrupted with general exception:", type(e), e)
    finally:
        timer.cancel()

interrupt_test()
