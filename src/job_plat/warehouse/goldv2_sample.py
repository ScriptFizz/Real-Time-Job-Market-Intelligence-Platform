

df = df.withColumn(
    "skill",
    explode("skills_normalized")
)

df = df.join(
    lookup_df,
    df.skill == lookup_df.raw_skill,
    how="left"
)
