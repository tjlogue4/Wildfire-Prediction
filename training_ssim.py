print('[INFO] loading packages...')
import os
import numpy as np
import h5py
from tqdm import tqdm
import glob
import csv

print('[INFO] loading tf...')

import tensorflow as tf
from tensorflow.keras.layers import Input
from tensorflow.keras.models import Model
from tensorflow.keras.optimizers import Adam
from tensorflow.keras import layers
from tensorflow.keras.callbacks import TensorBoard

physical_devices = tf.config.list_physical_devices('GPU')
tf.config.experimental.set_memory_growth(physical_devices[0], True)
#tf.config.experimental.set_memory_growth(physical_devices[1], True)
#tf.config.experimental.set_memory_growth(physical_devices[2], True)
#tf.config.experimental.set_memory_growth(physical_devices[3], True)

SIZE = (549, 549)
SHUFFLE = True
BATCH = 64
EPOCHS = 50
MODEL_NAME = 'model_13'
#fsx for lustre :)
PATH = '/home/ec2-user/fsx/splits/100/bool'
files = glob.glob(PATH + '/*.h5')
#files = glob.glob(PATH)
print(len(files))
'''
nn_files = []
for file in tqdm(files):
    r0 = file+"|"+'0'
    r90 = file+"|"+'90'
    r180 = file+"|"+'180'
    r270 = file+"|"+'270'
    rud = file+"|"+'ud'
    rlr = file+"|"+'lr'
    nn_files.extend([r0, r90, r180, r270, rud, rlr])

np.random.shuffle(nn_files)
train_files = nn_files[:300000]
test_files = nn_files[300000:450000]
new = nn_files[450000:]

with open(f'{MODEL_NAME}_new.csv', 'w') as f: 
    write = csv.writer(f) 
    write.writerow(new)
'''
start_file = 36000
end_file = 45000

np.random.shuffle(files)
train_files = files[:start_file]
test_files = files[start_file:end_file]
new = files[end_file:]

with open(f'{MODEL_NAME}_unseen.csv', 'w') as f: 
    write = csv.writer(f) 
    write.writerow(new)


    



print('[INFO] creating data pipeline...')

def make_dataset(filenames, batch_size):

    def load_file(filename):
        f = h5py.File(filename.numpy(), 'r', driver='core')
        B12 = f['B12'][:]
        NDVI = f['NDVI'][:]

        
        #not going to augment data at this time
        '''
        if r == "90":
            B12 = np.rot90(B12)
            NDVI = np.rot90(NDVI)
        elif r == "180":
            B12 = np.rot90(B12, 2)
            NDVI = np.rot90(NDVI, 2)
        elif r == "270":
            B12 = np.rot90(B12, 3)
            NDVI = np.rot90(NDVI, 3)
        elif r == "ud":
            B12 = np.flipud(B12)
            NDVI = np.flipud(NDVI)
        elif r == "lr":
            B12 = np.fliplr(B12)
            NDVI = np.fliplr(NDVI)
        '''
        # HxWxC Heigth width Channel
        NDVI = NDVI.reshape(SIZE[0], SIZE[1], 1)
        B12 = B12.reshape(SIZE[0], SIZE[1], 1)
        return NDVI, B12
    
    # cuz tensorflow
    def parse_file_tf(filename):
        return tf.py_function(load_file, [filename], [tf.float16, tf.float16])

    def configure_for_performance(ds, batch_size):
        ds = ds.shuffle(buffer_size=500)
        ds = ds.batch(batch_size)
        ds = ds.cache() # for instnces with greater than 60GB memory
        ds = ds.prefetch(buffer_size=tf.data.experimental.AUTOTUNE) # gets next batch ready
        ds = ds.repeat() # continuously loads
        return ds



    filenames_ds = tf.data.Dataset.from_tensor_slices(filenames)
    ds = filenames_ds.map(parse_file_tf, num_parallel_calls=tf.data.experimental.AUTOTUNE)
    ds = configure_for_performance(ds, batch_size)

    return ds

train_ds = make_dataset(train_files, BATCH)
test_ds = make_dataset(test_files, BATCH)


def ssim(y_true, y_pred):
    return 1 - tf.reduce_mean(tf.image.ssim(y_true, y_pred, 1.0))



print('[INFO] creating model...')

#strategy = tf.distribute.MirroredStrategy()
#print('Number of devices: {}'.format(strategy.num_replicas_in_sync))
#with strategy.scope():

loss_function = ssim
optimizer = tf.keras.optimizers.Adam()



inp = layers.Input(shape=(549, 549, 1))

x = layers.Conv2D(filters=32, kernel_size=(5,5), padding="same", activation="relu")(inp)
x = tf.keras.layers.MaxPooling2D(pool_size=(3, 3), padding="valid")(x)
x = layers.BatchNormalization()(x)


x = layers.Conv2D(filters=32, kernel_size=(3, 3), padding="same", activation="relu")(x)
x = tf.keras.layers.MaxPooling2D(pool_size=(3, 3), padding="valid")(x)
x = layers.BatchNormalization()(x)

x = layers.Conv2D(filters=32, kernel_size=(1, 1), padding="same", activation="relu")(x)
x = layers.BatchNormalization()(x)

x = layers.Conv2D(filters=32, kernel_size=(1, 1), padding="same", activation="relu")(x)
x = layers.BatchNormalization()(x)

x = layers.Conv2D(filters=32, kernel_size=(1, 1), padding="same", activation="relu")(x)
x = layers.BatchNormalization()(x)


x = layers.Conv2D(filters=81, kernel_size=(3, 3), padding="same", activation="sigmoid")(x)
x = tf.nn.depth_to_space(x, 9)



#Compiling the model
model = tf.keras.Model(inp, x)
model.compile(optimizer=optimizer, loss=loss_function)





tensorboard = TensorBoard(log_dir = f'logs/{MODEL_NAME}', profile_batch = 2 )
early_stopping = tf.keras.callbacks.EarlyStopping(monitor="val_loss", patience=5)
reduce_lr = tf.keras.callbacks.ReduceLROnPlateau(monitor="val_loss", patience=3)
model_checkpoint = tf.keras.callbacks.ModelCheckpoint(filepath=f'checkpoints/{MODEL_NAME}')

#Fit the model to the training data.
print('[INFO] fitting model...')
#model.fit(X, y, batch_size = BATCH, epochs=EPOCHS, validation_data=(Xt, yt), callbacks=[early_stopping, reduce_lr, tensorboard])
#model.fit(train_datagen, batch_size = BATCH, epochs=EPOCHS, validation_data=test_datagen, callbacks = [tensorboard,early_stopping, reduce_lr, model_checkpoint ])

model.fit(train_ds, epochs=EPOCHS, validation_data=test_ds,
          steps_per_epoch = len(train_files)// BATCH, validation_steps = len(test_files)// BATCH,
          callbacks=[early_stopping, reduce_lr, tensorboard, model_checkpoint])

print('[INFO] saving model...')
model.save(f'{MODEL_NAME}')

