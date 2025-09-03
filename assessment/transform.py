# assessment/transform.py
from pyspark.sql import DataFrame, functions as F, types as T


def compute(edges: DataFrame) -> DataFrame:
    """
    Input schema:
      parent STRING,
      child  STRING

    Output schema:
      node STRING,
      path STRING,
      depth LONG,
      descendants LONG,
      is_root BOOLEAN,
      is_leaf BOOLEAN
    """

    # ---- sanitize & dedupe ---------------------------------------------------
    edges = (
        edges.select(
            F.col("parent").cast("string").alias("parent"),
            F.col("child").cast("string").alias("child"),
        )
        .dropna()
        .dropDuplicates()
    )

    # Defensive: no edges => empty output
    if edges.limit(1).count() == 0:
        empty = edges.sql_ctx.createDataFrame([], schema=T.StructType([
            T.StructField("node", T.StringType()),
            T.StructField("path", T.StringType()),
            T.StructField("depth", T.LongType()),
            T.StructField("descendants", T.LongType()),
            T.StructField("is_root", T.BooleanType()),
            T.StructField("is_leaf", T.BooleanType()),
        ]))
        return empty

    # ---- find the single root (parents not listed as any child) --------------
    root_df = (
        edges.select(F.col("parent").alias("node")).distinct()
        .join(edges.select(F.col("child").alias("node")).distinct(), "node", "left_anti")
        .limit(1)
    )
    root_node = root_df.collect()[0][0]  # one root guaranteed by prompt

    # Build the universe of nodes (so we can attach flags reliably)
    nodes_all = (
        edges.select(F.col("parent").alias("node"))
        .union(edges.select(F.col("child").alias("node")))
        .distinct()
    )

    # ---- BFS to compute path & depth (iterative, fixpoint-safe) --------------
    # Represent path as array while iterating; join with '.' at the end.
    frontier = (
        nodes_all.where(F.col("node") == F.lit(root_node))
        .withColumn("path_elems", F.array(F.col("node")))
        .withColumn("depth", F.lit(0).cast("long"))
    )
    visited = frontier.select("node", "path_elems", "depth")

    max_iters = 20  # prompt says unknown but limited depth (~10); 20 is generous
    for _ in range(max_iters):
        # Expand one hop: parent(frontier.node) -> child
        nxt = (
            edges.join(frontier, edges.parent == frontier.node, "inner")
            .select(
                F.col("child").alias("node"),
                F.concat(frontier.path_elems, F.array(F.col("child"))).alias("path_elems"),
                (frontier.depth + F.lit(1)).cast("long").alias("depth"),
            )
            .dropDuplicates(["node"])  # shortest path retained naturally
        )

        # Do not revisit nodes already finalized
        nxt = nxt.join(visited.select("node"), "node", "left_anti")

        # Fixpoint check (cheap)
        if nxt.limit(1).count() == 0:
            break

        visited = visited.unionByName(nxt)
        frontier = nxt

    nodes_df = visited.withColumn("path", F.concat_ws(".", F.col("path_elems"))).drop("path_elems")

    # ---- flags: is_root / is_leaf -------------------------------------------
    is_root_df = nodes_all.withColumn("is_root", F.col("node") == F.lit(root_node))

    parents_only = edges.select(F.col("parent").alias("node")).distinct()
    is_leaf_df = (
        nodes_all.join(parents_only, "node", "left_anti")
        .withColumn("is_leaf", F.lit(True))
    )
    is_leaf_df = nodes_all.join(is_leaf_df, "node", "left").na.fill({"is_leaf": False})

    # ---- descendants via iterative transitive closure ------------------------
    # Start with direct (ancestor, descendant) pairs.
    anc_desc = (
        edges.select(
            F.col("parent").alias("ancestor"),
            F.col("child").alias("descendant"),
        )
        .dropDuplicates()
    )

    all_pairs = anc_desc
    frontier_pairs = anc_desc

    for _ in range(max_iters):
        # Extend: ancestor -> descendant -> child
        frontier_pairs = (
            frontier_pairs.join(
                edges, frontier_pairs.descendant == edges.parent, "inner"
            )
            .select(
                frontier_pairs.ancestor.alias("ancestor"),
                F.col("child").alias("descendant"),
            )
            .dropDuplicates()
        )

        # Stop if no new pairs
        if frontier_pairs.limit(1).count() == 0:
            break

        all_pairs = all_pairs.unionByName(frontier_pairs).dropDuplicates()

    descendants = (
        all_pairs.groupBy("ancestor").count()
        .withColumnRenamed("ancestor", "node")
        .withColumnRenamed("count", "descendants")
        .withColumn("descendants", F.col("descendants").cast("long"))
    )

    # ---- assemble final dataset ---------------------------------------------
    result = (
        nodes_df.join(is_root_df, "node", "left")
        .join(is_leaf_df, "node", "left")
        .join(descendants, "node", "left")
        .na.fill({"descendants": 0})
        .select(
            F.col("node").cast("string"),
            F.col("path").cast("string"),
            F.col("depth").cast("long"),
            F.col("descendants").cast("long"),
            F.col("is_root").cast("boolean"),
            F.col("is_leaf").cast("boolean"),
        )
        .orderBy("depth", "node")
    )

    return result
