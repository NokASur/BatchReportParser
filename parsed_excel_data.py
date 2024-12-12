from BatchStats.BasicBatchStats import BasicBatchStats


class ParsedExcelData:

    def __init__(self, batch_stats: list[BasicBatchStats], date: str, worker_name: str):
        self.batch_stats = sorted(batch_stats)
        self.date = date
        self.worker_name = worker_name

    def __str__(self):
        return (
            f"Report for {self.worker_name}:\n"
            f"Date: {self.date}\n"
            f"Batch Count: {self.worker_name}\n"
        )

    def __repr__(self):
        return (
                self.date + " " + self.worker_name
        )

    def type(self) -> str:
        if len(self.batch_stats) == 0:
            return "BasicBatchStats"
        return type(self.batch_stats[0]).__name__
