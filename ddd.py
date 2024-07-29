import sys
from ruamel.yaml import YAML
yaml=YAML()

print(sys.argv[1])
print(sys.argv[2])
print(sys.argv[3])

with open(sys.argv[3], 'r') as file:
    overrides_service = yaml.load(file)

new_env = { 'name': sys.argv[1], 'serviceAccountSecretRefs': { 'runtime': sys.argv[2]+"-apigee-runtime-sa", 'synchronizer': sys.argv[2]+"-apigee-synchronizer-sa" , 'udca': sys.argv[2]+"-apigee-udca-sa" } }

overrides_service['envs'].insert(0, new_env)


with open(sys.argv[3], 'w') as file:
    yaml.dump(overrides_service, file)
