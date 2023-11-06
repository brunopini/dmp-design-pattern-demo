from catalog import Local


if __name__ == '__main__':

    # Catalog instance from a local directory.
    catalog = Local('../bucket')

    # Iteration through Catalog audience generator attribute.
    # Each underlying Audience instance is automatically checking
    # for corresponding data file in the catalog. In its absense,
    # a DataSource attribute of the Audience instance fetches the
    # data and saves at Audience instance attribute level.
    for audience in catalog.audiences:
        # Checking for current POST status of each audience.
        if audience.adtech_a.status.value == 0:
            # Pushes custom payload to endpoint if new audience.
            audience.adtech_a.upload()

        # Repeat for every target destination.
        if audience.adtech_b.status.value == 0:
            audience.adtech_b.upload()

        # Update the catalog bucket with final updated states of all
        # audiences. New YAML and parquet.gz files are uploaded to the
        # bucket.
        catalog.push_state(audience)
