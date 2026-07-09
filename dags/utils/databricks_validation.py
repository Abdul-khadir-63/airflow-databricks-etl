from databricks import sql


def get_validation_result(
    server_hostname: str,
    http_path: str,
    access_token: str,
    pipeline_run_id: str,
):

    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    ) as connection:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT COUNT(*) AS failed_rules
                FROM workspace.default.validation_audit
                WHERE pipeline_run_id = ?
                  AND status = 'FAIL'
                """,
                (pipeline_run_id,),
            )

            fail_count = cursor.fetchone()[0]

            if fail_count == 0:
                return "PASS"

            return "FAIL"