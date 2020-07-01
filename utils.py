def get_bipartite_network(repo_id, time_from=None, time_to=None):
    engine_string = "postgresql+psycopg2://tianyi:nbanhl44@94.130.82.162:5432/tianyi"
    engine = create_engine(engine_string)
    sql = """
            SELECT commits.cmt_author_name as author_name, commits.cmt_filename as filename, commits.cmt_author_email as email, commits.cmt_author_timestamp as time
            FROM augur_data.commits as commits
            WHERE repo_id={} and length(cmt_filename)>1;
    """.format(repo_id)
    data = pd.read_sql(sql, engine)
    
    all_times = [dt for dt in data.time if not pd.isnull(dt)]
    if time_from == None:
        time_from = min(all_times)
    if time_to == None:
        time_to = max(all_times)
    
    node_info = {}
    edge_info = {}

    node_info['class'] = {}
    t = pp.TemporalNetwork()
    for idx, row in data.iterrows():
        if (row.time.replace(tzinfo=None) >= time_from) and \
                (row.time.replace(tzinfo=None) <= time_to):
            t.add_edge(row['author_name'], row['filename'].split('/')[-1], row['time'].replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S'), directed=True,
                       timestamp_format='%Y-%m-%d %H:%M:%S')
            node_info['class'][row['author_name']] = 'author'
            node_info['class'][row['filename'].split('/')[-1]] = 'file'
    
    return t, node_info, edge_info


def get_bipartite_network_from_commits(time_from=None, time_to=None):
    engine_string = "postgresql+psycopg2://tianyi:nbanhl44@94.130.82.162:5432/tianyi"
    engine = create_engine(engine_string)
    sql_commits_to_repo="""
        SELECT commits.repo_id as repo_id, commits.cmt_author_name as author_name, commits.cmt_author_timestamp as cmt_author_timestamp
        FROM augur_data.commits as commits
        WHERE cmt_author_timestamp>='{}'::timestamptz and cmt_author_timestamp < '{}'::timestamptz
    """.format(time_from, time_to)
    sql_repo_list ="""
            SELECT repo.repo_id as repo_id, repo.repo_name as repo_name, repo.repo_group_id as repo_group_id 
            FROM augur_data.repo as repo
    """
    result_commits_to_repo = pd.read_sql(sql_commits_to_repo, engine)
    result_repo_list = pd.read_sql(sql_repo_list,engine)
    engine.dispose()
    
    data = pd.merge(result_commits_to_repo,result_repo_list,left_on='repo_id',right_on='repo_id')
    
    node_info = {}
    edge_info = {}

    node_info['class'] = {}
    t = pp.TemporalNetwork()
    for idx, row in data.iterrows():
        t.add_edge(row['author_name'], row['repo_name'], row['cmt_author_timestamp'].replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S'), directed=True,
                       timestamp_format='%Y-%m-%d %H:%M:%S')
        node_info['class'][row['author_name']] = 'author'
        node_info['class'][row['repo_name']] = 'repo'
    
    return t, node_info, edge_info


def get_repo_author_count(node_info):
    repos=[]
    author = []
    for k,v in node_info['class'].items():
        if v == 'repo':
            repos.append(k)
        else:
            author.append(k)
    return [len(repos),len(author)]


def get_coauthorship_network_from_commits(time_from=None, time_to=None):
    engine_string = "postgresql+psycopg2://tianyi:nbanhl44@94.130.82.162:5432/tianyi"
    engine = create_engine(engine_string)
    sql_commits_to_repo="""
        SELECT commits.repo_id as repo_id, commits.cmt_author_name as author_name, commits.cmt_author_timestamp as cmt_author_timestamp
        FROM augur_data.commits as commits
        WHERE cmt_author_timestamp>='{}'::timestamptz and cmt_author_timestamp < '{}'::timestamptz
    """.format(time_from, time_to)
    sql_repo_list ="""
            SELECT repo.repo_id as repo_id, repo.repo_name as repo_name, repo.repo_group_id as repo_group_id 
            FROM augur_data.repo as repo
    """
    result_commits_to_repo = pd.read_sql(sql_commits_to_repo, engine)
    result_repo_list = pd.read_sql(sql_repo_list,engine)
    engine.dispose()
    
    data = pd.merge(result_commits_to_repo,result_repo_list,left_on='repo_id',right_on='repo_id')
    node_info = {}
    edge_info = {}
    n = pp.Network()
    for repo_id in data.repo_id.unique():
        n.add_clique(set(data.loc[data.repo_id == repo_id, 'author_name']))
   
    for edge in n.edges:
        if edge[0] == edge[1]:
            n.remove_edge(edge[0], edge[1])

    return n, node_info, edge_info