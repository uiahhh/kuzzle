Feature: Test REST API
  As a user
  I want to create/update/delete/search a document and test bulk import
  Using REST API

  @usingREST
  Scenario: Publish a realtime message
    When I publish a message
    Then I should receive a request id
    Then I'm not able to get the document
    And I'm not able to get the document in index "index-test-alt"

  @usingREST
  Scenario: Create a new document and get it
    When I write the document
    Then I should receive a document id
    Then I'm able to get the document

  @usingREST
  Scenario: Create or Update a document
    When I write the document
    And I createOrReplace it
    Then I should have updated the document

  @usingREST
  Scenario: Update a document
    When I write the document
    Then I update the document with value "foo" in field "firstName"
    Then my document has the value "foo" in field "firstName"

  @usingREST
  Scenario: Delete a document
    When I write the document
    Then I remove the document
    Then I'm not able to get the document

  @usingREST
  Scenario: Search a document
    When I write the document "documentGrace"
    Then I find a document with "grace" in field "firstName"
    And I don't find a document with "grace" in field "firstName" in index "index-test-alt"

  @usingREST
  Scenario: Bulk import
    When I do a bulk import
    Then I can retrieve actions from bulk import

  @usingREST
  Scenario: Global Bulk import
    When I do a global bulk import
    Then I can retrieve actions from bulk import

  @usingREST
  Scenario: Delete type
    When I write the document
    Then I remove the collection and schema
    Then I'm not able to get the document

  @usingREST
  Scenario: Count document
    When I write the document "documentGrace"
    When I write the document "documentAda"
    When I write the document "documentGrace"
    When I write the document "documentAda"
    Then I count 4 documents
    And I count 0 documents in index "index-test-alt"
    And I count 2 documents with "NYC" in field "city"
    Then I truncate the collection
    And I count 0 documents

  @usingREST
  Scenario: Change mapping
    When I write the document "documentGrace"
    Then I don't find a document with "Grace" in field "firstName"
    Then I remove the collection and schema
    Then I wait 1s
    Then I change the schema
    When I write the document "documentGrace"
    Then I find a document with "Grace" in field "firstName"

  @usingREST
  Scenario: Getting the last statistics frame
    When I get the last statistics frame
    Then I get at least 1 statistic frame

  @usingREST
  Scenario: Getting the statistics frame from a date
    When I get the statistics frame from a date
    Then I get at least 1 statistic frame

  @usingREST
  Scenario: Getting all statistics frame
    When I get all statistics frames
    Then I get at least 1 statistic frame

  @usingREST
  Scenario: list known stored collections
    When I write the document "documentGrace"
    And I list "stored" data collections
    Then I can find a stored collection kuzzle-collection-test

  @usingREST
  Scenario: list known realtime collections
    When I list "realtime" data collections
    Then I can not find a realtime collection

  @usingREST
  Scenario: get the Kuzzle timestamp
    When I get the server timestamp
    Then I can read the timestamp

  @usingREST
  Scenario: create additional index
    When I create an index named "my-new-index"
    Then I'm able to find the index named "my-new-index" in index list
    Then I'm not able to find the index named "my-undefined-index" in index list
    Then I'm able to delete the index named "my-new-index"

  @usingREST @cleanSecurity
  Scenario: Create/get/search/update/delete role
    When I create a new role "role1" with id "test"
    Then I'm able to find a role with id "test"
    And I update the role with id "test" with role "role2"
    Then I'm able to find a role with id "test" with role "role2"
    Then I'm able to find "1" role by searching index corresponding to role "role2"
    And I delete the role with id "test"
    Then I'm not able to find a role with id "test"
    Then I create a new role "role1" with id "test"
    And I create a new role "role1" with id "test2"
    And I create a new role "role1" with id "test3"
    Then I'm able to find "3" role by searching index corresponding to role "role1"
    Then I'm able to find "1" role by searching index corresponding to role "role1" from "0" to "1"

  @usingREST @cleanSecurity
  Scenario: create an invalid profile with unexisting role triggers an error
    Then I cannot create an invalid profile

  @usingREST @cleanSecurity
  Scenario: get profile without id triggers an error
    Then I cannot a profile without ID

  @usingREST @cleanSecurity
  Scenario: creating a profile with an empty set of roles triggers an error
    Then I cannot create a profile with an empty set of roles

  @usingREST @cleanSecurity
  Scenario: create, get and delete a profile
    Given I create a new role "role1" with id "role1"
    And I create a new role "role2" with id "role2"
    When I create a new profile "profile1" with id "my-new-profile"
    Then I'm able to find the profile with id "my-new-profile"
    Given I delete the profile with id "my-new-profile"
    Then I'm not able to find the profile with id "my-new-profile"

  @usingREST @cleanSecurity
  Scenario: search and update profiles
    Given I create a new profile "profile1" with id "my-profile-1"
    And I create a new profile "profile3" with id "my-profile-2"
    Then I'm able to find "1" profiles containing the role with id "role1"
    Then I'm able to find "2" profiles
    Then I'm able to find "0" profiles containing the role with id "undefined-role"
    Given I update the profile with id "my-profile-2" by adding the role "role1"
    Then I'm able to find "2" profiles
    Then I'm able to find "2" profiles containing the role with id "role1"
