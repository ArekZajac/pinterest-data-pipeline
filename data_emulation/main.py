import data_emulation
import data_ingestion

def clock(core: data_emulation.Core, output: str) -> None:
    while True:
        data = core.run_emulation_cycle()
        if data:
            match output:
                case "batch":
                    batch_processor = data_ingestion.BatchIngestor()
                    batch_processor.to_msk(data["pin"], data["geo"], data["user"])
                case "stream":
                    stream_processor = data_ingestion.StreamIngestor()
                    stream_processor.to_kinesis(data["pin"], data["geo"], data["user"])
                case "console":
                    core.to_console(data["pin"], data["geo"], data["user"])

if __name__ == "__main__":
    core_instance = data_emulation.Core()
    clock(core_instance, "stream")
