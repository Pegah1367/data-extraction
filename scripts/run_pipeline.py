import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # allow "from src ..." when run directly

from pathlib import Path
import argparse
from src import data_extraction as dx

def main():
    p = argparse.ArgumentParser(description="Run data extraction")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("csv").add_argument("--path", required=True)
    ex = sub.add_parser("excel"); ex.add_argument("--path", required=True); ex.add_argument("--sheet", default=0)
    ap = sub.add_parser("api"); ap.add_argument("--url", required=True)
    js = sub.add_parser("json"); js.add_argument("--path", required=True)

    mg = sub.add_parser("mongo")
    mg.add_argument("--uri", required=True)
    mg.add_argument("--db", required=True)
    mg.add_argument("--coll", required=True)
    mg.add_argument("--limit", type=int)

    my = sub.add_parser("mysql")
    my.add_argument("--host", required=True)
    my.add_argument("--user", required=True)
    my.add_argument("--password", required=True)
    my.add_argument("--db", required=True)
    my.add_argument("--query", required=True)
    my.add_argument("--port", type=int, default=3306)

    sc = sub.add_parser("scrape"); sc.add_argument("--url", required=True)

    p.add_argument("--out", default="data/output.csv")
    args = p.parse_args()
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)

    if args.cmd == "csv":
        df = dx.load_csv(args.path)
    elif args.cmd == "excel":
        df = dx.load_excel(args.path, sheet_name=args.sheet)
    elif args.cmd == "api":
        df = dx.load_api(args.url)
    elif args.cmd == "json":
        df = dx.load_json(args.path)
    elif args.cmd == "mongo":
        df = dx.load_mongodb(args.uri, args.db, args.coll, limit=args.limit)
    elif args.cmd == "mysql":
        df = dx.load_mysql(args.host, args.user, args.password, args.db, args.query, port=args.port)
    elif args.cmd == "scrape":
        df = dx.scrape_shop(args.url)
    else:
        raise SystemExit("Unknown command")

    df.to_csv(out, index=False)
    print(f"? Saved {len(df):,} rows ? {out}")

if __name__ == "__main__":
    main()