https://dev.azure.com/WBA/EAP/_build?definitionId=22057



ado-pl-apigee-manage-environments.yml

resources:
  repositories:
  - repository: cicd-dev-automation
    type: git
    name: EAP/cicd-dev-automation
    ref: 'master'

  - repository: apigee-hybrid-platform
    type: git
    name: EAP/apigee-hybrid-platform
    ref: 'master'
    
parameters:
- name: apigeeOrganization
  type: string
  values:
  - nprod-01-intseg-eapi
  - prod-01-intseg-eapi

- name: apigeeEnvironment
  type: string
  default: '-'
- name: apigeeEnvironmentGroup
  type: string
  default: '-'
- name: apigeeEnvironmentDescription
  type: string
  default: '-'

- name: apigeeEnvironmentTicket
  type: string
  default: '-'

trigger: none

extends:
  template: /Templates/ado/ado-tl-create-apigee-environment.yaml
  parameters:
    apigeeOrganization: ${{ lower(parameters.apigeeOrganization) }}
    apigeeEnvironment: ${{ lower(parameters.apigeeEnvironment) }}
    apigeeEnvironmentGroup: ${{ lower(parameters.apigeeEnvironmentGroup) }}
    apigeeEnvironmentDescription: ${{ parameters.apigeeEnvironmentDescription }}
    apigeeEnvironmentTicket: ${{ parameters.apigeeEnvironmentTicket }}
