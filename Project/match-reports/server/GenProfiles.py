import build_profiles_catapult

# No need to do much in here yet, because building profiles just updates sql
def build_profiles_handler():

    build_profiles_catapult()

    # NEXT:
    # build_profiles_VALD


# RUN FILE
build_profiles_handler()