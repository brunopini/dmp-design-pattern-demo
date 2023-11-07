# Python Design Pattern - Data Management Platform

Design patterns similar to this keep coming up for some of the latest Martech solutions I've been building, so I felt like sharing it in case anyone sees value.

``` mermaid
graph LR;
    Bucket <-- state.yml<br/>data.parquet.gz --> Catalog
    Catalog -- generates --> Audience
    Audience <-- connection --> Adtech
    Audience <-- connection --> DataSource
    Audience -- updates --> Catalog
```

## Context and Challenge

- Marketing need to have first party audiences set up and uploaded to different adtechs.
- All adtechs must share the same source-of-truth data.
- Quality of input data, and of payload being sent, need to validated.
- Resource efficiency is key:
  - Memory efficiency for parsing as many audiences of all sizes.
  - Minimal data source access, shielding it from unnecessary data requests.
- No-code state configuration files, avoiding code adjustments when new audiences are set up.

## Solution

- A bucket hosting both audience data and state config files.
  - One YAML state config file per audience, configuring all audience components:
    - Audience metadata.
    - Data source parameters.
    - Adtechs parameters.
    - Last response information on all components.
- 4 Python classes working in tandem:
  - `Catalog` is the abtract class that represents any catalog source, such as a `Local` directory.
    - The `Catalog` class has a generator attribute `audiences` that processes a single audience information at a time, preserving memory.
  - `Audience` is the concrete class that serves as the glue of the whole process. It is instantiated in the `Catalog` generator method, and connects the state config files in the bucket with the readily available data. and instantiates in itself the adtech instances.
    - As attributes, each `audience` instance instantiates `Adtech` and `DataSource` classes.
  - `DataSource` is an abstract class that represents any data source, such as an `ApiGateway`. From the `audience` instance attribute, `DataSource` is responsible for fetching data that is not yet available in the bucket - this logic is called only when needed.
  - `Adtech` is an abstract class that represents any adtech. On concrete class definitions, payload constructors and API specific methods are customized based on the abstractmethods provided and specific API documentations.

### Generator Workflow - `Catalog.audiences`

The `Catalog` class will instantiate - one at a time - the `Audience` class with a single `state.yml` from the bucket. By using a generator method to define the `audiences` attribute of `Catalog`, the script will dedicate its resources to the processing of one instance of `Audience` at a time, scrapping any leftover memory consumption. This means the program is now much more scalable and can opperate with high volume base audiences.

For each iteration of the generator, this is the overview of the actors and actions that will be triggered:

``` mermaid
sequenceDiagram
    participant Bucket
    participant Catalog
    participant Audience
    participant DataSource
    participant Adtech
    loop Generator
        Bucket->>Catalog: Fetch state.yml<br/>& data.parquet.gz
        Catalog->>Audience: Instantiate<br/>Audience(state, data)
        Audience->>DataSource: Extract data if<br/>missing data
        DataSource->>Audience: Update state
        Audience->>Adtech: Instantiate Adtech(state, data)
        Audience->>Adtech: Upload audience if not yet uploaded
        Adtech->>Audience: Update state
        Audience->>Catalog: Update state
        Catalog->>Bucket: Update state<br/>& data files
    end
```

### Tree

``` bash
.
├── bucket
│   ├── data
│   │   └── demo_audience.parquet.gz
│   └── state
│       └── demo_audience.yml
└── dmp
    ├── _utils.py
    ├── adtechs
    │   ├── _adtech.py
    │   ├── adtechA.py
    │   └── adtechB.py
    ├── datasource
    │   ├── _datasource.py
    │   └── apigateway.py
    ├── audience.py
    ├── catalog.py
    └── main.py
```

### ER Diagram of Class Relationships

The relationship between all classes involved in the process with greater detail on each attribute and interaction.

``` mermaid
erDiagram
    Catalog ||--o{ Audience : generates
    Catalog {
        client bucket "Client object of bucket provider"
        generator[Audience] audiences "Generator of Audience instances"
    }
    Audience ||--o{ Member : lists
    Member {
        list[SHA256] email
        list[SHA256] phone_number
        list[SHA256] zip_code
    }
    Audience ||--o{ Adtech : instantiates
    Audience {
        string name "Unique name from YAML state file"
        string description "Extracted from state file"
        dict state "Dynamically updated state"
        DataSource source "DataSource instance"
        list[Member] members "List of Audience.Member instances"
        Adtech adtech "Adtech instance"
        dict state "Dynamically updated state, referencing components' states"
    }
    Adtech {
        string audience_id "Extracted from API response after upload"
        string audience_name "Extracted from state file"
        string audience_description "Extracted from state file"
        list[dict] audience_members "Passed from Audience instance"
        Status[Enum] status "Audience status in the Adtech server"
        API api "Instance of API innerclass"
        dict payload "Constructed payload"
        dict response "Response of the Aftech server API call"
        dict state "Dynamically updated state"
    }
    Adtech ||--|| Status : checks
    Status {
        NOT_FETCHED value "-1"
        NOT_POSTED value "0"
        POSTED value "1"
    }
    Adtech ||--|| API : uploads
    API {
        string endpoint
        dict headers
        auth token
    }
    Audience ||--|| DataSource : extracts
    DataSource {
        client source "Client object of source provider"
        string endpoint "Extracted from state file"
        dict params "Extracted from state file"
        dict response "Response of the Aftech server API call"
        dict state "Dynamically updated state"
    }
```

### `main.py` execution

``` python
from catalog import Local

catalog = Local('../bucket')

for audience in catalog.audiences:
    if audience.adtech_a.status.value == 0:
        audience.adtech_a.upload()

    if audience.adtech_b.status.value == 0:
        audience.adtech_b.upload()

    catalog.push_state(audience)
```

## Docs

___

### Audience States

An audience's state is a YAML file with the meta data of that specific audience, identified both by its main key `demo_audience` and its file name `demo_audience.yml` (both need match and be unique for each audience in the catalog bucket).

The file structure is composed of blocks related to source and adtech configuration parameters. Each `Adtech` concrete class init argument `state` is tied to one specific configuration block.

#### 1. Basic Configuration

In this file, basic configuration for a new audience can be set:

- The endpoint of the data source and its parameters for data extraction are set.
- `AdtechA` is configured, including its parameter `audience_type`, which is set to one of the possible Enum values declared in `AtechA.AudienceType` class.

``` yaml
demo_audience:
  description: A demo audience from a filtered demo data source.

  source:
    endpoint: demo-endpoint/
    params:
      demo: true

  adtechA:
    name: New Audience
    audience_type: TYPE_X
```

This state file now has two blocks: `source` and `adtechA`. Each block will serve as a state condiguration for, resppectively, an `source` and an `AdtechA` instances.

___

#### 2. First Execution

During a first run, the script will catalog this new state file alongside all available in the `state` bucket directory. During processing of the states, the routine will fetch any data that is not yet available in the bucket using the `source` source block config. Once data is available, the audience will be uploaded to `AdtechA`, and extra information will be set at the state file, which is uploaded back to the bucket, alongside the newly created data file `demo_audience.parquet.gz` at the `data` directory of the bucket.

After this first run, the updated state file will look like:

``` yaml
demo_audience:
  description: A demo audience from a filtered demo data source.

  source:
    endpoint: demo-endpoint/
    params:
      demo: true
    last_response:
      date: '20231106'
      status: 200
      message: 'Success: Demo response message.'

  adtechA:
    name: Demo Audience
    id: '1234567890'
    audience_type: TYPE_X
    last_response:
      date: '20231106'
      status: 200
      message: 'Success: Demo response message.'
```

Now that the audience was created in ***AdtechA***, future runs will neither request data nor upload to it again.

___

#### 3. Further Configurations

This state file can be later reconfigured to include `AdtechB` (with its own special parameter `expiration_time` set to one of the possible Enum values declared in `AdtechB.ExpirationTime`):

``` yaml
demo_audience:
  description: A demo audience from a filtered demo data source.

  source:
    endpoint: demo-endpoint/
    params:
      demo: true
    last_response:
      date: 20231106
      status: 200
      message: 'Success: Demo response message.'

  adtechA:
    name: Demo Audience
    id: 1234567890
    audience_type: TYPE_X
    last_response:
      date: 20231106
      status: 200
      message: 'Success: Demo response message.'

  adtechB:
    name: Demo Audience
    expiration_time: 300
```

After a second run, the audience will access its available data, avoiding requesting from data source again, and upload it to `AdtechB`, resulting in this final state file.

``` yaml
demo_audience:
  description: A demo audience from a filtered demo data source.

  source:
    endpoint: demo-endpoint/
    params:
      demo: true
    last_response:
      date: 20231106
      status: 200
      message: 'Success: Demo response message.'

  adtechA:
    name: Demo Audience
    id: 1234567890
    audience_type: TYPE_X
    last_response:
      date: 20231106
      status: 200
      message: 'Success: Demo response message.'

  adtechB:
    name: Demo Audience
    id: 0987654321
    expiration_time: 300
    last_response:
      date: 20231107
      status: 200
      message: 'Success: Demo response message.'
```

### Data

For most first-party audience sharing purposes, adtechs overlap in best match rates for PIIs such as emails, phone numbers and zip codes.

For a simple unified process of audience sharing, these were are three fields expected by the `Audience.Member` inner class.

``` python
class Member(BaseModel):

    email: list
    phone_number: list
    zip_code: list
```

The class performs some validations to account for privacy, match efficiency and code uniformity, such as:

- Checking for SHA256 hashing, and encrypting if not yet hashed.
- Normalization of values prior to hashing.
- Formatting fields as multi-value lists, as some adtechs allow for this.

___

### `Adtech` Concrete Class Definitions

When defining a concrete class of `Adtech`, custom behavior is implemented as new arguments, inner classes and methods.

#### Attributes & Inner Classes

##### `AdtechA`

In this demo, `AdtechA` has a specific parameter `audience_type`. This attribute is mapped to the state config key:

``` python
class AdtechA(Adtech):
    def __init__(
            self, name: str, description: str,
            state: dict, member_records: list[dict]) -> None:
        ...
        self.audience_type = self.AudienceType[
            str(state.get('audience_type')).upper()]
```

An inner Enum class `AdtechA.AudienceType` was used to ensure correct setup of this parameter:

``` python
@unique
class AudienceType(Enum):
    TYPE_X = 'TYPE_X'
```

This is mapped to the corresponding `state.yml` file block:

``` yaml
demo_audience:
  ...
  adtechA:
    ...
    audience_type: TYPE_X
```

##### `AdtechB`

`AdtechB` also has a specific parameter mapped:

``` python
class AdtechB(Adtech):
    def __init__(
            self, name: str, description: str,
            state: dict, member_records: list[dict]) -> None:
        ...
        self.expiration_time = self.ExpirationTime(
            state.get('expiration_time'))
```

This time, an inner class `AdtechB.ExpirationTime` is implemented to ensure valid values are passed:

``` python
class ExpirationTime:
    VALID_DURATION = set(range(541)) | {1_000}

    def __init__(self, value: int | str = 1000) -> None:
        if int(value) not in self.VALID_DURATION:
            raise ValueError(
                'Invalid expiration time value. Must be between 0 and'
                ' 540 days, or exactly 1000 if no expiration date.'
            )
        self.value = int(value)
```

#### Payloads

Payload construction happens in the `_format_payload` method of given `Adtech` instance.

Customization of this also expects a map to the state file configurations, preferably acessing new and dedicated instance attributes, such as the `AdtechB` example:

``` python
def _format_payload(self) -> dict:
    payload = {
        "name": self.audience_name,
        "description": self.audience_description,
        "expiration": self.expiration_time.value
    }

    def _inject_members(payload) -> dict:

        schema = [
            "EMAIL",
            "PHONE",
            "ZIP"
        ]

        data = [
            [
                dct["email"],
                dct["phone_number"],
                dct["zip_code"]]
            for dct in self.member_records
        ]

        payload.update({
            "schema": schema,
            "data": data
        })
        return payload

    def _drop_empty_keys(dct: dict) -> dict:
        new_dict = {}
        for key, value in dct.items():
            if isinstance(value, dict):
                value = _drop_empty_keys(value)
            elif isinstance(value, list):
                value = [v for v in value if v]
                if not value:
                    continue
            if value:
                new_dict[key] = value
        return new_dict

    payload = _inject_members(payload)
    payload = _drop_empty_keys(payload)
    return payload
```

Customizing this method when creating new `Adtech` concrete classes is crucial.

#### API Configuration

API calls are concentrated in the inner class `Adtech.API`. There, authentication mechanisms need to be implemented, as well as defining correct request behavior and elements, as illustrated with `AdtechA`:

``` python
class API(Adtech.API):
    _API_VERSION = 'v2'
    _ENDPOINT = ('https://{version}/?advertiserId={advertiserId}')
    _HEADERS = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": "Bearer {access_token}"
    }

    def __init__(self, credentials: dict = None) -> None:
        _advertiser_id = credentials['advertiser_id']
        self.headers: dict = {
            k: v.format(access_token=credentials['access_token'])
            for k, v in self._HEADERS.items()
        }
        self.endpoint: str = self._ENDPOINT.format(
            version=AdtechA.API._API_VERSION,
            advertiserId=str(_advertiser_id)
        )

    def post(self, payload: dict) -> requests.Response:
        ...
        return response
```

The post method of the `API` class is called directly from the `Adtech` parent instance:

``` python
class AdtechA(Adtech):
    ...
    def upload(self) -> None:
        response = self.api.post(self.payload)
        self.response = response
        return response
```

___

### `DataSource` Concrete Class Definitions

Similarly to the process of defining custom concrete `Adtech` classes, `DataSource` concrete class definitions follow the same steps:

1. Map attributes to state configs.
2. Adapt methods to handle specific behaviors.

> Originally, this code only expects one `DataSource` instance type connected every `Audience` instance, at `self.source` attribute, but this can be customized if needed.

___

## Demo

For this demo, onde audience state file `demo_audience.yml` is placed in the [local bucket state directory](./bucket/state/). A corresponding data file `demo_audience.parquet.gz`is placed in the [local bucket data directory](./bucket/data/).

Two concrete classes of `Adtech` were defined: `AdtechA` and `AdtechB`. These will illustrate two different data destinations.

To better illustrate the code behavior, a [TRYME.ipynb](./dmp/TRYME.ipynb) is provided in this repo.

___

## Collaboration

Please feel free to collaborate and help improve this!
