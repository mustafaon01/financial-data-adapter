"""Seed initial tenants and users."""

from django.contrib.auth import get_user_model

from api.models import Client, UserTenant

User = get_user_model()


def ensure_client(code, name):
    """Create or update a tenant."""
    """
    Check tenant exist if not, then create
    """
    obj, created = Client.objects.get_or_create(
        tenant_code=code,
        defaults={"name": name},
    )
    if not created and obj.name != name:
        obj.name = name
        obj.save(update_fields=["name"])
    return obj


def ensure_user(username, password, is_superuser=False):
    """Create or update a user."""
    new_user = User.objects.filter(username=username).first()
    if not new_user:
        new_user = User.objects.create_user(username=username, password=password)
        print(f"created user: {username}")
    else:
        new_user.set_password(password)
        print(f"updated password: {username}")
    if is_superuser:
        new_user.is_staff = True
        new_user.is_superuser = True
    else:
        new_user.is_staff = False
        new_user.is_superuser = False
    new_user.save()
    return new_user


def ensure_link(user, tenant):
    """Link user to tenant."""
    link = UserTenant.objects.filter(user=user).first()
    if not link:
        UserTenant.objects.create(user=user, tenant=tenant)
        print(f"linked {user.username} -> {tenant.tenant_code}")
    else:
        if link.tenant_id != tenant.id:
            link.tenant = tenant
            link.save(update_fields=["tenant"])
            print(f"relinked {user.username} -> {tenant.tenant_code}")
        else:
            print(f"link ok {user.username} -> {tenant.tenant_code}")


def run():
    """Seed tenants and users."""
    bank1 = ensure_client("BANK001", "Bank 001")
    bank2 = ensure_client("BANK002", "Bank 002")
    bank3 = ensure_client("BANK003", "Bank 003")

    ensure_user("admin", "admin123", is_superuser=True)
    u1 = ensure_user("bank001_user", "test123", is_superuser=False)
    u2 = ensure_user("bank002_user", "test123", is_superuser=False)
    u3 = ensure_user("bank003_user", "test123", is_superuser=False)

    ensure_link(u1, bank1)
    ensure_link(u2, bank2)
    ensure_link(u3, bank3)

    print("\nDONE")
    print("admin => admin123 (superuser)")
    print("bank001_user => test123 (BANK001)")
    print("bank002_user => test123 (BANK002)")
    print("bank003_user => test123 (BANK003)")


if __name__ == "__main__":
    run()
