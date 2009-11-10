# Set up sample data for IndexBuilder example
create "people", "attributes"
create "people-email", "INDEX"
create "people-phone", "INDEX"
create "people-name", "INDEX"

[["1", "jenny", "jenny@example.com", "867-5309"],
 ["2", "alice", "alice@example.com", "555-1234"],
 ["3", "kevin", "kevinpet@example.com", "555-1212"]].each do |fields|
  (id, name, email, phone) = *fields
  put "people", id, "attributes:name", name
  put "people", id, "attributes:email", email
  put "people", id, "attributes:phone", phone
end
  
