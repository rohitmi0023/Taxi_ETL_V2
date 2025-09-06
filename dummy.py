class B(Exception):
    pass

class C(B):
    pass

class D(C):
    pass

for cls in [B, C, D]:
    print(f'class {cls}')
    try:
        raise cls()
    except D:
        print('D')
    except C:
        print('C')
    except B:
        print('B')

print('='*50)

for cls in [B, C, D]:
    print(f'Class {cls}')
    try:
        raise cls()
    except B:
        print('B')
    except C:
        print('C')
    except D:
        print('D')
