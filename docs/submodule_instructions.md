# Submodule Instructions  

This file contains a list of instructions about how to manage submodules used in this project.

## Add Submodules

This instruction describes how to add necessary submodules.

At first, you need to ensure all submodules are cleaned. You may follow the instructions in the `Clean Submodules` section to clean all submodules.

- Add submodule configs and clone submodules
  - `git submodule add -b main https://github.com/GreptimeTeam/greptime-proto contrib/greptime-proto`
  - `git submodule add -b v1.46.x https://github.com/grpc/grpc third_party/grpc`
  - the above commands make local directories track given branches of remote repositories
  - the `.gitmodules` file will present corresponding content if all goes well
- Initialize all submodules recursively
  - `git submodule update --init --recursive --remote`
  - this command will initialize all submodules and their submodules recursively, i.e. all required submodules will be cloned into local directories and will checkout to the default main/master branch or the given branch if you have specified in the `git submodule add` command with the `-b` option.
- Ensure all submodules are ready
  - `cd third_party/grpc`
  - `git pull --recurse-submodules`
  - occasionally, some indirect submodules, submodules needed by our submodules, won't be cloned or checked out properly due to network issues. In such cases, you need to execute the above command to pull all submodules of the `grpc` submodule.
  - note, the `greptime-proto` submodule does not have any nested submodules, so you don't need to take care of it.

## Clean Submodules

This instruction describes how to clean all submodules.

- Remove source files of submodules:  
  - `rm -rf contrib/greptime-proto`
  - `rm -rf third_party/grpc`
- Remove git configs of submodules:  
  - `cd .git`
  - `vim config`
  - delete relevant lines of submodule configs
- Remove git metadata of submodules:  
  - `cd .git`
  - `rm -rf modules`
- Ensure the `.gitmodules` file is cleaned
- Add and commit changes
  - `git add .`
  - `git commit -m "clean submodules"`
- Check submodule status to ensure all submodules are cleaned
  - `git submodule status`
  - all cleaned if this command dumps nothing
