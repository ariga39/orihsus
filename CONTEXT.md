# Domain glossary

## Gateway key

An OpenCode Go subscription credential that orihsus may select for an upstream request.

## Key failure handling

The policy applied after an upstream request fails because of the selected gateway key. It controls temporary backoff, circuit breaking, and whether the request may be attempted with another key.

## Key rotation

The act of selecting a different gateway key. Rotation is an outcome of key failure handling, not the name of the failure policy itself.
