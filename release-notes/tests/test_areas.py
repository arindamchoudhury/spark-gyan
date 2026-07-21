from catalog.areas import assign_area, AREAS, load_overrides

SLUGS = {s for s, _ in AREAS}

def test_all_areas_have_unique_slugs():
    slugs = [s for s, _ in AREAS]
    assert len(slugs) == len(set(slugs))
    assert "misc" in slugs

def test_keyword_routing():
    assert assign_area("SPARK-1", "Structured Streaming watermark support", {}) == "structured-streaming"
    assert assign_area("SPARK-2", "pandas API on Spark: add DataFrame.corr", {}) == "pandas-on-spark"
    assert assign_area("SPARK-3", "Spark Connect: support foreachBatch", {}) == "spark-connect"
    assert assign_area("SPARK-4", "Kubernetes: executor pod template", {}) == "deploy"

def test_override_wins_over_keywords():
    ov = {"SPARK-5": "geospatial"}
    # title looks like SQL, but override forces geospatial
    assert assign_area("SPARK-5", "add ST_Distance to SQL functions", ov) == "geospatial"

def test_unmatched_falls_back_to_misc():
    assert assign_area("SPARK-6", "zzz unclassifiable text", {}) == "misc"
