import report_catapult
import report_vald

# No need to do much in here yet, because building profiles just updates sql
def generate_report_handler():

    catapult_report_df = report_catapult.get_catapult_report_metrics_main()

    forcedecks_report_df, nordbord_report_df = report_vald.get_vald_report_metrics_main()

    # FRONTEND / VISUALIZATION CODE GOES HERE (or calls to helper fns)

    print("Report generation complete")
    print(f"Catapult data: {len(catapult_report_df)} players")
    print(f"ForceDecks data: {len(forcedecks_report_df)} players")
    print(f"NordBord data: {len(nordbord_report_df)} players")
    return


# RUN FILE
generate_report_handler()