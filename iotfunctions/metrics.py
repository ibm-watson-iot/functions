# *****************************************************************************
# © Copyright IBM Corp. 2018, 2025  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import numpy as np
import torch

#
# Transformation Invariant Loss function with Distance EQuilibrium
#  https://arxiv.org/abs/2210.15050
#  https://github.com/HyunWookL/TILDE-Q
# as metric for forecasting accuracy
#  TODO: we need a suitable threshold to differentiate "good" and "bad" forecasts
#
#  Last dimension is time for outputs and targets
#

def loss_helper(outputs, targets, fn, axis=1):

    #print(type(targets), type(outputs))
    #print(targets.shape, outputs.shape)

    # make sure output and target have the same dtype
    outputs = outputs.astype(np.float64)

    # and get rid of unused dimensions
    targets = targets.squeeze()
    outputs = outputs.squeeze()

    if len(targets.shape) == 0 or targets.shape[0] == 0:
        print('Nothing to compare')
        return np.array(0.0)

    if len(targets.shape) == 1:
        if targets.shape[0] < 4: return np.array([0.0])

        return fn(torch.from_numpy(targets.reshape(1,-1)),
                  torch.from_numpy(outputs.reshape(1,-1))).cpu().detach().item()
    
    B, T = targets.shape  # batch, time

    # and we focus on the time dimension here
    # using an array to allow for different statistics than just 'mean' along non-time axes (no Queen of Heart jokes,please !)

    if axis == 1:
        if T < 4:
            print('Too short to compare')
            return np.array([0.0])

        l_fn = fn(torch.from_numpy(targets),
                     torch.from_numpy(outputs)).cpu().detach().numpy() #.item()
        return l_fn
    
    return np.array(0.0)

def TILDEQ(outputs, targets, axis=1):
    return loss_helper(outputs, targets, tildeq_loss, axis=axis)

def TILDEQ_AMP(outputs, targets, axis=1):
    return loss_helper(outputs, targets, amp_loss, axis=axis)

def TILDEQ_ASHIFT(outputs, targets, axis=1):
    return loss_helper(outputs, targets, ashift_loss, axis=axis)

def TILDEQ_PHASE(outputs, targets, axis=1):
    return loss_helper(outputs, targets, phase_loss, axis=axis)

#
### Time dimension is last
#
def tildeq_loss(target, output):
    amp = amp_loss(target, output)
    shift = ashift_loss(target, output)
    phase = phase_loss(target, output)
    loss = 0.5 * phase + 0.5 * shift + 0.01 * amp
    #print('TILDE-Q', loss.shape, torch.mean(loss))
    return loss

def amp_loss(outputs, targets):
    B, T = outputs.shape

    fft_size = 1 << (2 * T - 1).bit_length()
    out_fourier = torch.fft.fft(outputs, fft_size, dim = -1)
    tgt_fourier = torch.fft.fft(targets, fft_size, dim = -1)

    out_norm = torch.linalg.vector_norm(outputs, dim = -1)
    tgt_norm = torch.linalg.vector_norm(targets, dim = -1)

    #calculate normalized auto correlation
    auto_corr = torch.fft.ifft(tgt_fourier * tgt_fourier.conj(), dim = -1).real

    # drop first coefficient to remove the mean
    auto_corr = torch.cat([auto_corr[...,-(T-1):], auto_corr[...,:T]], dim = -1)
   
    # avoid 0/0 -> nan
    norm = torch.where(tgt_norm == 0, 1e-9, tgt_norm * tgt_norm)

    # normalize with the target norm along batch dimension
    nac_tgt = auto_corr / norm.unsqueeze(1)

    # calculate cross correlation
    cross_corr = torch.fft.ifft(tgt_fourier * out_fourier.conj(), dim = -1).real
    cross_corr = torch.cat([cross_corr[...,-(T-1):], cross_corr[...,:T]], dim = -1)

    # again avoid 0/0 -> nan
    norm = torch.where(tgt_norm * out_norm == 0, 1e-9, tgt_norm * out_norm)
    nac_out = cross_corr / (tgt_norm * out_norm).unsqueeze(1)

    loss = torch.mean(torch.abs(nac_tgt - nac_out), dim = -1)
    #print('AMP-Q', loss.shape, torch.mean(loss))
    return loss

def ashift_loss(outputs, targets):
    B, T = outputs.shape
    loss = T * torch.mean(torch.abs(1 / T - torch.softmax(outputs - targets, dim = -1)), dim = -1)
    #print('SHIFT-Q', loss.shape, torch.mean(loss))
    return loss

def phase_loss(outputs, targets):
    B, T = outputs.shape
    out_fourier = torch.fft.fft(outputs, dim = -1)
    tgt_fourier = torch.fft.fft(targets, dim = -1)
    tgt_fourier_sq = (tgt_fourier.real ** 2 + tgt_fourier.imag ** 2)
   
    # find the dominant frequences of the target
    mask = (tgt_fourier_sq > (T)).float()
    topk_indices = tgt_fourier_sq.topk(k = int(T**0.5), dim = -1).indices
    mask = mask.scatter_(-1, topk_indices, 1.)
    mask[...,0] = 1.
    mask = torch.where(mask > 0, 1., 0.)
    mask = mask.bool()
    not_mask = (~mask).float()
    not_mask /= torch.mean(not_mask, dim=-1).unsqueeze(1)

    # act on it
    out_fourier_sq = (torch.abs(out_fourier.real) + torch.abs(out_fourier.imag))
    zero_error = torch.abs(out_fourier) * not_mask
    zero_error = torch.where(torch.isnan(zero_error), torch.zeros_like(zero_error), zero_error)
    mask = mask.float()
    mask /= torch.mean(mask, dim=-1).unsqueeze(1)
    ae = torch.abs(out_fourier - tgt_fourier) * mask
    ae = torch.where(torch.isnan(ae), torch.zeros_like(ae), ae)

    loss = (torch.mean(zero_error, dim=-1) + torch.mean(ae,dim=-1)) / (T ** .5)
    #print('PHASE-Q', loss.shape, torch.mean(loss))
    return loss


#
# Time series forecasting with trend loss function
#  https://download.ssrn.com/pr22/62733efa-f686-4135-afc5-65f68049e490-meca.pdf
#
#  Last dimension is time for outputs and targets
#
def TREND(targets, outputs, axis=1):
    return loss_helper(outputs, targets, trend_loss, axis=axis)

    
#
#  https://github.com/liaohaibing/Trend-learning-loss-function
# as metric for forecasting accuracy
#  (replaced scipy cdist and numpy corrcoef with torch cdist and cosine similarity for readability)
#
def trend_loss(targets,outputs,alpha=0.5,device='cpu'):

    sq_error = w_mse(targets,outputs,device)

    error1=torch.mean(sq_error, dim=-1)

    x1 = derivatives(targets, device)
    x2 = derivatives(outputs, device)

    # center by rows first
    _xt1 = x1.squeeze()
    _xt2 = x2.squeeze()

    if len(_xt1.shape) == 1:
        _xt1 = torch.reshape(_xt1,(1,_xt1.shape[0]))

    batch_size, lens = _xt1.shape[0:2]

    _xcentered1 = None
    _xcentered2 = None
    if len(_xt1.shape) > 1:
        _xt1 = _xt1.T
        _xt2 = _xt2.T
        _xcentered1 = (_xt1 - _xt1.mean(dim=0)).T
        _xcentered2 = (_xt2 - _xt2.mean(dim=0)).T
    else:
        _xcentered1 = _xt1 - _xt1.mean()
        _xcentered2 = _xt2 - _xt2.mean()


    # apply cosine similarity along the time dimension
    p_corr = torch.nn.functional.cosine_similarity(_xcentered1, _xcentered2, dim=-1)

    w_corr = 1 - p_corr
    
    dd = torch.norm(targets - outputs, dim=-1)

    d_error = w_corr * dd
    
    return error1 + alpha * d_error


def w_mse(targets,outputs,device='cpu'):

    batch_size, lens = targets.shape[0:2]

    target1= targets[:,1:lens].to(device)
    target2= targets[:,0:lens-1].to(device)
    output1 = outputs[:, 1:lens].to(device)
    output2 = outputs[:, 0:lens - 1].to(device)
    
    sigma_matrix=(target1-target2)*(output1-output2).to(device)
    sigma=torch.tanh(sigma_matrix).to(device)

    newtargets=targets[:,1:lens].to(device)
    newoutputs=outputs[:,1:lens].to(device)

    # implemented as described in the paper
    w_error = torch.abs(newoutputs-newtargets)*(1.0-sigma).to(device)

    return w_error

'''
def w_mse_original(targets,outputs,device='cpu'):

    delta=0.00000001
    batch_size, lens = targets.shape[0:2]

    target1= targets[:,1:lens].to(device)
    target2= targets[:,0:lens-1].to(device)
    output1 = outputs[:, 1:lens].to(device)
    output2 = outputs[:, 0:lens - 1].to(device)

    sigma_matrix=(target1-target2)*(output1-output2).to(device)
    
    # paper proposes tanh, but the author's implementation resorts to simple signum
    sigma=torch.sign(sigma_matrix).to(device)

    newtargets=targets[:,1:lens].to(device)
    newoutputs=outputs[:,1:lens].to(device)

    # not sure why the implementation makes use of an elementwise power
    W=(1.01+torch.abs(newoutputs-newtargets)/(torch.abs(newoutputs+newtargets)+delta))**(1.0-sigma).to(device)

    w_error=W*torch.abs(newoutputs-newtargets)

    return w_error
'''

def derivatives(Input,device='cpu'):

        batch_size,lens =Input.shape[0:2]

        input2 = Input[:,2:lens].to(device)
        input1 = Input[:,0:lens-2].to(device)

        return input2 - input1


'''
FORECASTING CLASSICAL METRICS
'''

def RMSE(y_true,y_pred,axis = None):
    if axis is None:
        values = np.mean((y_true-y_pred)**2)
    else:
         values = np.mean((y_true-y_pred)**2,axis = axis)
    return np.sqrt(values)

def MAE(y_true,y_pred,axis = None):
    if axis is None:
        values = np.mean(np.abs(y_true-y_pred))
    else:
         values = np.mean(np.abs(y_true-y_pred),axis = axis)
    return values

def NRMSE(y_true,y_pred,axis = None,norm = 'mean'):
    values = RMSE(y_true,y_pred,axis = axis)
    if norm == 'minmax':
        den = (np.max(y_true)-np.min(y_true))
    else:
        den = np.mean(y_true)
    return np.sqrt(values)/np.abs(den)

def MAPE(y_true,y_pred,axis = None):
    # print(y_true.shape, y_pred.shape)
    # Create a mask for non-zero true values
    non_zero_mask = np.array(y_true != 0).astype('int')
    y_true_denominator = np.array(y_true)*non_zero_mask + (1-non_zero_mask)*1e-15
    values = np.abs((y_true-y_pred))/np.abs(y_true_denominator)

    if axis is None:
        if np.sum(non_zero_mask) > 0:
            return np.sum(values*non_zero_mask)/np.sum(non_zero_mask)*100
        else:
            # print('ALL TRUE VALUES ARE ZERO :', non_zero_mask.shape,non_zero_mask )
            return None
    else:
        if np.sum(non_zero_mask) > 0:
            numerator = np.sum(values*non_zero_mask,axis = axis)
            denominator = np.sum(non_zero_mask,axis = axis)
            output = 100*numerator
            output[denominator == 0] = None
            output[denominator != 0] /=denominator[denominator != 0]
            return output
        else:
            # print('ALL TRUE VALUES ARE ZERO :', non_zero_mask.shape)
            return None

def SMAPE(y_true,y_pred,axis = None):
    '''
    mean( |y_true-y_pred|2/(|y_true|+|y_pred|)) 
    if y_true = 0 SMAPE = 2
    '''

    denominator = (np.abs(y_true) + np.abs(y_pred))/2
    non_zero_mask = np.array(denominator != 0).astype('int')
    denominator = np.array(denominator)*non_zero_mask + (1-non_zero_mask)*1e-15
    numerator = np.abs(y_true-y_pred)
    values = numerator/denominator

    if axis is None:
        return np.mean(values)*100
    else:
        return np.mean(values,axis = axis)*100

def WAPE(y_true,y_pred,axis = None):

    # Create a mask for non-zero true values
    # non_zero_mask = np.array(y_true != 0).astype('int')
    numerator = np.abs((y_true-y_pred))
    if axis is None:
        denominator = np.sum(np.abs(y_true))
        if denominator == 0:
            return None
        else:
            values = np.sum(numerator)/denominator
            return values
    else:
        denominator = np.sum(np.abs(y_true),axis = axis)
        # non_zero_mask = np.array(denominator != 0).astype('int')
        # denominator = denominator*non_zero_mask #+ (1-non_zero_mask)*1e-15
        values = np.sum(numerator,axis = axis) #/denominator
        values[denominator == 0] = None
        values[denominator != 0] /=denominator[denominator != 0]
        return values

def Bias(y_true,y_pred,axis = None):
    delta = y_pred-y_true
    if axis is None:
        return np.mean(delta)
    else:
        return np.mean(delta,axis=axis)
    
def cosine_similarity_matrix(A, B,axis=1):
    dot_product = np.sum(A * B, axis=axis)
    norm_A = np.linalg.norm(A, axis=axis)
    norm_B = np.linalg.norm(B, axis=axis)
    return dot_product / (norm_A * norm_B)


METRICS_FORECAST = {}
METRICS_FORECAST['RMSE'] = RMSE
METRICS_FORECAST['MAE'] = MAE
METRICS_FORECAST['MAPE'] = MAPE
METRICS_FORECAST['SMAPE'] = SMAPE
METRICS_FORECAST['WAPE'] = WAPE
METRICS_FORECAST['Bias'] = Bias
METRICS_FORECAST['NRMSE'] = NRMSE
# METRICS_FORECAST['PEARSONCORR'] = pearson_corr
METRICS_FORECAST['TREND'] = TREND
METRICS_FORECAST['TILDEQ'] = TILDEQ
METRICS_FORECAST['COSSIM'] = cosine_similarity_matrix
