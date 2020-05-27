from openstack import connection

if __name__ == "__main__":
    flavors = {}
    auth_args = {
        'auth_url':'http://252.3.51.106:5000/v3',
        'project_name':'admin',
        'domain_name' : 'admin_domain',
        'username':'admin',
        'password':'openstack',
    }
    conn = connection.Connection(**auth_args)
    for server in conn.compute.servers():
        print(server.name)
    for image in conn.compute.images():
        flavors[image.name] = image.id
        print(image.name)
    for flavor in conn.compute.flavors():
        print(flavor.name)

     server = conn.compute.create_server(
        name="test-vm-creation", image_id=image.id, flavor_id=flavor.id,
        networks=[{"uuid": network.id}], key_name=keypair.name)
