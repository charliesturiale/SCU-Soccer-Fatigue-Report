import report_catapult

# No need to do much in here yet, because building profiles just updates sql
def generate_report_handler():

    catapult_report_df = report_catapult.get_catapult_report_metrics_main()

    # NEXT: Get VALD report

    # FRONTEND / VISUALIZATION CODE GOES HERE (or calls to helper fns)

    print("Report generation complete")
    print(f"Catapult data: {len(catapult_report_df)} players")
    return


# RUN FILE
generate_report_handler()