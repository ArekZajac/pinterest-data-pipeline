import data_emulation
import batch_ingestion
import stream_ingestion

def clock(core, output):
        while True:
            data = core.run_emulation_cycle(core)
            if data:
                match output:
                    case "batch":
                        batch_ingestion.BatchIngestor.to_msk(data["pin"], data["geo"], data["user"])
                    case "stream":
                        stream_ingestion.StreamIngestor.to_kinesis(data["pin"], data["geo"], data["user"])
                    case "console":
                        core.to_console(data["pin"], data["geo"], data["user"])

if __name__ == "__main__":
    core_instance = data_emulation.Core()
    clock(core_instance, "console")