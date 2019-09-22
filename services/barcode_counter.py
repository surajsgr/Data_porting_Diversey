

from collections import namedtuple

__BarCodeEntityInfo__ = namedtuple("__BarCodeEntityInfo__", "entity_name prefixes")


class BarCodeEntity(object):
    """
    Class BarCodeEntity
    This class is used to define barcode entity.
    """

    CLIENT = __BarCodeEntityInfo__(
        entity_name='client',
        prefixes=dict(
            barcode='M'
        )
    )
    CUSTOMER = __BarCodeEntityInfo__(
        entity_name='customer',
        prefixes=dict(
            barcode='C'
        )
    )
    OBJECT = __BarCodeEntityInfo__(
        entity_name='building',
        prefixes=dict(
            barcode='O'
        )
    )
    ROOM = __BarCodeEntityInfo__(
        entity_name='room',
        prefixes=dict(
            barcode='R'
        )
    )
    PERSONAL_EMPLOYEE = __BarCodeEntityInfo__(
        entity_name='personal_employee',
        prefixes=dict(
            barcode='P'
        )
    )
    TASK_CODE = __BarCodeEntityInfo__(
        entity_name='task',
        prefixes=dict(
            barcode='T',
            second_liner_barcode='S',
            second_liner_done_barcode='U'
        )
    )
    MAINTENANCE = __BarCodeEntityInfo__(
        entity_name='maintenance',
        prefixes=dict(
            maintenance_barcode='W',
            maintenance_done_barcode='X'
        )
    )

    @staticmethod
    def get_barcode(bc_entity, hex_code, start_stop=0):
        return {f_name: '*' + (prefix + str(start_stop) + str(hex_code).zfill(5)) + '*'
                for f_name, prefix in bc_entity.prefixes.items()}
