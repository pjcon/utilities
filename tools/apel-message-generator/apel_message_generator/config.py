# Defaults
defaults = {
    'message_format':'apel',
    'records_per_message':1000,
    'number_of_message':1000,
    'message_dir':None,
}

# Arbitrary strings for filling messages.
sample_strings = '''Site Navigation
Home
Latest Information
Email Hoaxes
Internet Scams
Previous Issues
Site FAQs
Hoax-Slayer Social
HS About
Privacy Policy
HS Site Map
True Emails
Virus Hoaxes
Giveaway Hoaxes
Charity Hoaxes
Bogus Warnings
Email Petitions
Chain Letters
Unsubstantiated
Missing Child Hoaxes'''.splitlines()

# Some example DNs
dns = ['/C=UK/O=eScience/OU=CLRC/L=RAL/CN=apel-dev.esc.rl.ac.uk/emailAddress=sct-certificates@stfc.ac.uk',
       '/C=UK/O=eScience/OU=CLRC/L=RAL/CN=apel-consumer2.esc.rl.ac.uk/emailAddress=sct-certificates@stfc.ac.uk',
       '/c=cy/o=cygrid/o=hpcl/cn=mon101.grid.ucy.ac.cy',
       '/c=hu/o=niif ca/ou=grid/ou=niif/cn=host/egi1.grid.niif.hu',
       '/dc=org/dc=balticgrid/ou=mif.vu.lt/cn=host/grid9.mif.vu.lt',
       '/dc=es/dc=irisgrid/o=pic/cn=mon01.pic.es',
       '/dc=ro/dc=romaniangrid/o=ifin-hh/cn=tbit03.nipne.ro']

# Possible acceptable values representing null
null_values = ['NULL', 'Null', 'null', 'NONE', 'None', 'none', '']
