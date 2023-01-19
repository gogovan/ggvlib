PERSON = {
    "$schema": "http://json-schema.org/draft-04/schema",
    "type": "object",
    "properties": {
        "names": {
            "type": "array",
            "items": [
                {
                    "type": "object",
                    "properties": {"givenName": {"type": "string"}},
                    "required": ["givenName"],
                }
            ],
        },
        "phoneNumbers": {
            "type": "array",
            "items": [
                {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                }
            ],
        },
    },
    "required": ["names"],
}
PERSON_BATCH = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "contacts": {
            "type": "array",
            "items": [
                {
                    "type": "object",
                    "properties": {
                        "contactPerson": {
                            "type": "object",
                            "properties": {
                                "names": {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "object",
                                            "properties": {
                                                "givenName": {"type": "string"}
                                            },
                                            "required": ["givenName"],
                                        }
                                    ],
                                }
                            },
                            "required": ["names"],
                        }
                    },
                    "required": ["contactPerson"],
                }
            ],
        }
    },
    "required": ["contacts"],
}
