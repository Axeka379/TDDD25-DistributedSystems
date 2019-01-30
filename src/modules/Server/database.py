# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 24 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Implementation of a simple database class."""

import random


class Database(object):

    fortune_list = list()
    """Class containing a database implementation."""
    def __init__(self, db_file):
        self.db_file = db_file
        self.rand = random.Random()
        self.rand.seed()
        #
        # Your code here.
        #
        temporary_string = ""

        file = open(db_file, "r+")

        for line in file:
            if line != "%\n":
                temporary_string += line
            else:
                self.fortune_list.append(temporary_string)
                temporary_string = ""
        file.close()

    def read(self):
        """Read a random location in the database."""
        #
        # Your code here.
        #

        if self.fortune_list:
            return random.choice(self.fortune_list)


    def write(self, fortune):
        """Write a new fortune to the database."""
        #
        # Your code here.
        #
        self.fortune_list.append(fortune)

        file = open(self.db_file, "a+")
        file.write(fortune)
        file.write("\n%\n")
        file.close()
