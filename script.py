import sqlite3, os, json, argparse
import pandas as pd
import networkx as nx
from pathlib import Path
def load_df_tasks_from_db(db_path = None, db_filename = "monitoring.db"):
    run_dir = "./runinfo"
    df_tasks = None
    df_wf = None
    if db_path == None:
        db_path =  os.path.abspath(run_dir) # if the path is not given, search for the monitoring.db in the current's folder runinfo
    
    monitoring_db_file = os.path.join(db_path, db_filename)

    if os.path.exists(monitoring_db_file):
        try:
            with sqlite3.connect(monitoring_db_file) as connection:
                # get data from tasks
                query_tasks = f"SELECT * FROM task"
                df_tasks = pd.read_sql_query(query_tasks, connection)
                df_tasks = df_tasks[df_tasks['task_time_returned'].notnull() & df_tasks['task_time_invoked'].notnull()] #select only the items with valid timestamps
                df_tasks['task_time_returned'] = pd.to_datetime(df_tasks['task_time_returned'], errors='coerce')
                df_tasks['task_time_invoked'] = pd.to_datetime(df_tasks['task_time_invoked'], errors='coerce')
                df_tasks = df_tasks[df_tasks['task_time_returned'].notna() & df_tasks['task_time_invoked'].notna()] #drop NaT items
                df_tasks.loc[:, 'runtime'] = df_tasks['task_time_returned'] - df_tasks['task_time_invoked']
                df_tasks.loc[:, 'runtime_seconds'] = df_tasks['runtime'].dt.total_seconds()
                df_tasks = df_tasks[df_tasks['runtime_seconds'] >= 0]
                # get data from workflow
                select_query = f"SELECT * FROM workflow"
                df_wf = pd.read_sql_query(select_query, connection)
                df_wf = df_wf[df_wf['time_began'].notnull() & df_wf['time_completed'].notnull()] #select only the items with valid timestamps
                df_wf['time_completed'] = pd.to_datetime(df_wf['time_completed'], errors='coerce')
                df_wf['time_began'] = pd.to_datetime(df_wf['time_began'], errors='coerce')
                df_wf = df_wf[df_wf['time_began'].notna() & df_wf['time_completed'].notna() & (df_wf["tasks_failed_count"] == 0)] #drop NaT items
                df_wf.loc[:, 'runtime'] = df_wf['time_completed'] - df_wf['time_began']
                df_wf.loc[:, 'runtime_seconds'] = df_wf['runtime'].dt.total_seconds()
                df_wf = df_wf[df_wf['runtime_seconds'] >= 0]

                return df_tasks, df_wf
        except:
                return None, None
        
def load_graph(run_id, df):
    dag = nx.DiGraph()
    df_run = df[df["run_id"] == run_id]
    df_run = df_run.sort_values(by=['task_id'], ascending=[True])
    if df_run.empty:
        return None
    else:
        tasks = df_run[["task_id", "task_func_name", "runtime_seconds", "task_depends","task_time_invoked"]]
        for i, r in tasks.iterrows():
            task_id = r["task_id"]
            task_func_name = r["task_func_name"]
            runtime = r["runtime_seconds"]
            time_invoked = str(r["task_time_invoked"])
            depends_on = (r["task_depends"]).split(',')
            depends_on = list(filter((lambda x : len(x)>0), depends_on))
            dag.add_node(task_id, task_func_name=task_func_name, runtime=runtime, time_invoked=time_invoked)
            for d in depends_on:
                dag.add_edge(int(d), task_id)

            # Add source and sink nodes to the dag
            sources = [n for n in dag if dag.in_degree(n) == 0]
            sinks = [n for n in dag if dag.out_degree(n) == 0]

            dag.add_node(-1, task_func_name="source", runtime=0)
            dag.add_node(-2, task_func_name="sink", runtime=0)

            for s in sources:
                dag.add_edge(-1, s)
            for t in sinks:
                dag.add_edge(t, -2)
        return dag
    
def create_wfcommon(df_tasks, df_wf, run_id, output_file):
    #TODO: get the resource info
    dag = load_graph(run_id, df_tasks)
    row = df_wf.loc[df_wf["run_id"] == run_id].iloc[0]
    uuid = row["run_id"]
    name = row["workflow_name"]
    wf_created_at = str(row["time_began"])
    wf_runtime = row["runtime_seconds"]
    tasks = list() # list of tasks for the specification
    tasks_exec = list() # list of tasks for the execution
    nodes = dag.nodes(data=True)
    for id, n in nodes:
        tasks.append(
            {"name": n["task_func_name"],
             "id": str(id),
             "parents": [str(i) for i in dag.predecessors(id)],
             "children": [str(i) for i in dag.successors(id)]
             }
        )
        tasks_exec.append(
            {"name": n["task_func_name"],
            "id": str(id),
            "runtimeInSeconds": n["runtime"]
            }
        )
    wf_json = {
        "name": name,
        "description": "TOBEFILLED",
        "createdAt": wf_created_at,
        "schemaVersion": '1.5',
        "workflow": {
            "specification": {
                "tasks": tasks,
                "files": list()
            },
            "execution": {
                "makespanInSeconds": wf_runtime,
                "executedAt": wf_created_at,
                "tasks": tasks_exec,
            }
        },
        "author": {
            "name": "TOBEFILLED",
            "email": "TOBEFILLED",
            "institution": "TOBEFILLED",
            "country": "TOBEFILLED"
        },
        "runtimeSystem": {
            "name": "Parsl",
            "url": "https://parsl-project.org/",
            "version": "1.2"
        }
    }
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(wf_json, f, ensure_ascii=False, indent=4)
        print(f"Workflow {run_id} dumped on {output_file}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Parsl monitoring to WFCommons",
        description="Script to format the data on Parsl's monitoring database for a valid WFCommons format"
    )
    parser.add_argument("-p", "--path", help="Database path", required=False, default=None)
    parser.add_argument("-i", "--input", help="Database filename", required=False, default="monitoring.db")
    parser.add_argument("-r", "--runid", help="Run ID", required=False)
    parser.add_argument("-o", "--output", help="Output JSON file", required=True)
    args = parser.parse_args()

    # Load dataframes from database
    df_tasks, df_wf = load_df_tasks_from_db(db_path=args.path, db_filename=args.input)
    if df_tasks is None or df_wf is None:
        print("Error: Could not load tasks or workflow data from the database.")
        exit(1)
    if args.runid:
        create_wfcommon(df_tasks, df_wf, args.runid, args.output)
    else:
        # Process all runs for all IDs
        run_ids = df_wf["run_id"].tolist()
        output_base = Path(args.output).with_suffix("")
        for run_id in run_ids:
            outfile = output_base.with_name(f"{output_base.name}_{run_id}.json")
            create_wfcommon(df_tasks, df_wf, run_id, outfile)