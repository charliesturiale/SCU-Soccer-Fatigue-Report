import build_profiles_catapult
import build_profiles_vald

# No need to do much in here yet, because building profiles just updates sql
def build_profiles_handler():

    build_profiles_catapult.build_profiles_main()
    build_profiles_vald.build_profiles_main()


# RUN FILE
if __name__ == "__main__":
    build_profiles_handler()